//go:generate ../../../tools/readme_config_includer/generator
package exec

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

const maxStderrBytes = 512

// Exec defines the exec output plugin.
type Exec struct {
	Command        []string        `toml:"command"`
	Environment    []string        `toml:"environment"`
	Timeout        config.Duration `toml:"timeout"`
	UseBatchFormat bool            `toml:"use_batch_format"`
	Log            telegraf.Logger `toml:"-"`

	runner     Runner
	serializer telegraf.Serializer
}

func (*Exec) SampleConfig() string {
	return sampleConfig
}

func (e *Exec) Init() error {
	e.runner = &CommandRunner{log: e.Log}

	return nil
}

// SetSerializer sets the serializer for the output.
func (e *Exec) SetSerializer(serializer telegraf.Serializer) {
	e.serializer = serializer
}

// Connect satisfies the Output interface.
func (*Exec) Connect() error {
	return nil
}

// Close satisfies the Output interface.
func (*Exec) Close() error {
	return nil
}

// Write writes the metrics to the configured command.
func (e *Exec) Write(metrics []telegraf.Metric) error {
	return e.WriteContext(context.Background(), metrics)
}

// WriteContext writes the metrics to the configured command. It returns
// promptly once ctx is cancelled: the in-flight subprocess (if any) is
// killed and reaped in the background rather than being waited on.
func (e *Exec) WriteContext(ctx context.Context, metrics []telegraf.Metric) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var buffer bytes.Buffer
	if e.UseBatchFormat {
		serializedMetrics, err := e.serializer.SerializeBatch(metrics)
		if err != nil {
			return err
		}
		buffer.Write(serializedMetrics)

		if buffer.Len() <= 0 {
			return nil
		}

		return e.runner.Run(ctx, time.Duration(e.Timeout), e.Command, e.Environment, &buffer)
	}
	errs := make([]error, 0, len(metrics))
	for _, metric := range metrics {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}

		serializedMetric, err := e.serializer.Serialize(metric)
		if err != nil {
			return err
		}
		buffer.Reset()
		buffer.Write(serializedMetric)

		err = e.runner.Run(ctx, time.Duration(e.Timeout), e.Command, e.Environment, &buffer)
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Runner provides an interface for running exec.Cmd.
type Runner interface {
	Run(ctx context.Context, timeout time.Duration, command, environment []string, buffer io.Reader) error
}

// CommandRunner runs a command with the ability to kill the process before the timeout.
type CommandRunner struct {
	cmd *exec.Cmd
	log telegraf.Logger
}

// Run runs the command, bounded both by the plugin's own configured
// timeout (which sends SIGTERM then SIGKILL after a grace period, see
// internal.WaitTimeout) and by ctx. If ctx is cancelled first, the process
// is killed outright and reaped in a background goroutine so Run returns
// promptly without waiting for it; the caller's ctx.Err() is returned in
// that case instead of an internal-timeout error.
func (c *CommandRunner) Run(ctx context.Context, timeout time.Duration, command, environments []string, buffer io.Reader) error {
	cmd := exec.Command(command[0], command[1:]...)
	if len(environments) > 0 {
		cmd.Env = append(os.Environ(), environments...)
	}
	cmd.Stdin = buffer
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() {
		done <- internal.WaitTimeout(cmd, timeout)
	}()

	var err error
	select {
	case err = <-done:
	case <-ctx.Done():
		// The caller is no longer waiting; kill the process outright
		// (skipping the internal SIGTERM grace period, since nothing is
		// waiting to observe a clean shutdown) and reap it in the
		// background so this call can return immediately.
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		go func() { <-done }()
		return ctx.Err()
	}
	s := stderr

	if err != nil {
		if errors.Is(err, internal.ErrTimeout) {
			return fmt.Errorf("%q timed out and was killed", command)
		}

		s = removeWindowsCarriageReturns(s)
		if s.Len() > 0 {
			if c.log.Level() < telegraf.Debug {
				c.log.Errorf("Command error: %q", truncate(s))
			} else {
				c.log.Debugf("Command error: %v", s)
			}
		}

		if status, ok := internal.ExitStatus(err); ok {
			return fmt.Errorf("%q exited %d with %w", command, status, err)
		}

		return fmt.Errorf("%q failed with %w", command, err)
	}

	c.cmd = cmd

	return nil
}

func truncate(buf bytes.Buffer) string {
	// Limit the number of bytes.
	didTruncate := false
	if buf.Len() > maxStderrBytes {
		buf.Truncate(maxStderrBytes)
		didTruncate = true
	}
	if i := bytes.IndexByte(buf.Bytes(), '\n'); i > 0 {
		// Only show truncation if the newline wasn't the last character.
		if i < buf.Len()-1 {
			didTruncate = true
		}
		buf.Truncate(i)
	}
	if didTruncate {
		buf.WriteString("...")
	}
	return buf.String()
}

func init() {
	outputs.Add("exec", func() telegraf.Output {
		return &Exec{
			Timeout:        config.Duration(time.Second * 5),
			UseBatchFormat: true,
		}
	})
}

// removeWindowsCarriageReturns removes all carriage returns from the input if the
// OS is Windows. It does not return any errors.
func removeWindowsCarriageReturns(b bytes.Buffer) bytes.Buffer {
	if runtime.GOOS == "windows" {
		var buf bytes.Buffer
		for {
			byt, err := b.ReadBytes(0x0D)
			byt = bytes.TrimRight(byt, "\x0d")
			if len(byt) > 0 {
				buf.Write(byt)
			}
			if errors.Is(err, io.EOF) {
				return buf
			}
		}
	}
	return b
}
