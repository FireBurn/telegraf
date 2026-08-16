//go:generate ../../../tools/readme_config_includer/generator
package execd

import (
	"bufio"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/internal/process"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

type Execd struct {
	Command                  []string        `toml:"command"`
	Environment              []string        `toml:"environment"`
	RestartDelay             config.Duration `toml:"restart_delay"`
	IgnoreSerializationError bool            `toml:"ignore_serialization_error"`
	UseBatchFormat           bool            `toml:"use_batch_format"`
	Log                      telegraf.Logger

	process    *process.Process
	serializer telegraf.Serializer

	// writeSem is a 1-buffered channel acting as a cancellable mutex around
	// the subprocess's stdin pipe. A write acquires it before writing and
	// returns it once the write to stdin returns. If a write is abandoned
	// because its ctx was cancelled while stdin.Write was still blocked
	// (see writeContext), the token is only returned once that abandoned
	// write eventually completes - so a genuinely wedged subprocess causes
	// every subsequent write to fail the same way (fail fast on their own
	// ctx) rather than piling up concurrent writers on the same pipe.
	writeSem chan struct{}
}

func (*Execd) SampleConfig() string {
	return sampleConfig
}

func (e *Execd) SetSerializer(s telegraf.Serializer) {
	e.serializer = s
}

func (e *Execd) Init() error {
	if len(e.Command) == 0 {
		return errors.New("no command specified")
	}

	var err error

	e.process, err = process.New(e.Command, e.Environment)
	if err != nil {
		return fmt.Errorf("error creating process %s: %w", e.Command, err)
	}
	e.process.Log = e.Log
	e.process.RestartDelay = time.Duration(e.RestartDelay)
	e.process.ReadStdoutFn = e.cmdReadOut
	e.process.ReadStderrFn = e.cmdReadErr

	e.writeSem = make(chan struct{}, 1)
	e.writeSem <- struct{}{}

	return nil
}

func (e *Execd) Connect() error {
	if err := e.process.Start(); err != nil {
		// if there was only one argument, and it contained spaces, warn the user
		// that they may have configured it wrong.
		if len(e.Command) == 1 && strings.Contains(e.Command[0], " ") {
			e.Log.Warn("The outputs.execd Command contained spaces but no arguments. " +
				"This setting expects the program and arguments as an array of strings, " +
				"not as a space-delimited string. See the plugin readme for an example.")
		}
		return fmt.Errorf("failed to start process %s: %w", e.Command, err)
	}

	return nil
}

func (e *Execd) Close() error {
	e.process.Stop()
	return nil
}

// Write writes the metrics to the long-running subprocess's stdin.
func (e *Execd) Write(metrics []telegraf.Metric) error {
	return e.WriteContext(context.Background(), metrics)
}

// WriteContext writes the metrics to the long-running subprocess's stdin.
// The subprocess is never killed or restarted here - only the persistent
// child process managed by internal/process.Process is; killing it on every
// cancelled write would be a much bigger behavior change than this
// conversion is meant to make (see plugin README). Instead, each write to
// the pipe races against ctx and is abandoned (but left running in the
// background) on cancellation. See writeContext for what abandonment means
// for subsequent writes.
func (e *Execd) WriteContext(ctx context.Context, metrics []telegraf.Metric) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if e.UseBatchFormat {
		b, err := e.serializer.SerializeBatch(metrics)
		if err != nil {
			return fmt.Errorf("error serializing metrics: %w", err)
		}
		return e.writeContext(ctx, b)
	}
	for _, m := range metrics {
		if err := ctx.Err(); err != nil {
			return err
		}

		b, err := e.serializer.Serialize(m)
		if err != nil {
			if !e.IgnoreSerializationError {
				return fmt.Errorf("error serializing metrics: %w", err)
			}
			e.Log.Errorf("Skipping metric due to a serialization error: %v", err)
			continue
		}

		if err := e.writeContext(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

// writeContext writes b to the subprocess's stdin, racing the pipe write
// against ctx cancellation per the goroutine-race-and-abandon pattern in
// docs/specs/tsd-012-output-context-aware-write.md. A pipe write can block
// indefinitely if the subprocess is alive but stuck/deadlocked and not
// consuming stdin.
//
// If ctx is cancelled first, this write is abandoned: the goroutine keeps
// running against the pipe in the background, and the subprocess itself is
// left completely untouched (not killed, not restarted). writeSem is only
// returned once that abandoned write eventually completes, so it is not
// released while a write may still be in flight against the pipe. This
// means: if the subprocess is genuinely wedged, every subsequent write also
// fails (once its own ctx/write_timeout expires) rather than piling up
// concurrent writers on the same pipe - the pipe recovers on its own only
// if the subprocess starts consuming stdin again, or via
// internal/process.Process's restart-on-exit logic if the subprocess dies
// outright. Cancellation here does not fix a wedged subprocess; it only
// stops Telegraf from blocking forever on it.
func (e *Execd) writeContext(ctx context.Context, b []byte) error {
	select {
	case <-e.writeSem:
	case <-ctx.Done():
		return ctx.Err()
	}

	done := make(chan error, 1)
	stdin := e.process.Stdin
	go func() {
		_, err := stdin.Write(b)
		done <- err
		e.writeSem <- struct{}{}
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("error writing metrics: %w", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Execd) cmdReadErr(out io.Reader) {
	scanner := bufio.NewScanner(out)

	for scanner.Scan() {
		e.Log.Errorf("stderr: %s", scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		e.Log.Errorf("Error reading stderr: %s", err)
	}
}

func (e *Execd) cmdReadOut(out io.Reader) {
	scanner := bufio.NewScanner(out)

	for scanner.Scan() {
		e.Log.Info(scanner.Text())
	}
}

func init() {
	outputs.Add("execd", func() telegraf.Output {
		return &Execd{}
	})
}
