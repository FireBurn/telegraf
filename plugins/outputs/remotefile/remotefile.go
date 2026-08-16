//go:generate ../../../tools/readme_config_includer/generator
package remotefile

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"text/template"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fspath"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/common/slog"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

type File struct {
	Remote               config.Secret   `toml:"remote"`
	Files                []string        `toml:"files"`
	FinalWriteTimeout    config.Duration `toml:"final_write_timeout"`
	WriteBackInterval    config.Duration `toml:"cache_write_back"`
	MaxCacheSize         config.Size     `toml:"cache_max_size"`
	UseBatchFormat       bool            `toml:"use_batch_format"`
	ForgetFiles          config.Duration `toml:"forget_files_after"`
	CompressionAlgorithm string          `toml:"compression_algorithm"`
	CompressionLevel     int             `toml:"compression_level"`
	Log                  telegraf.Logger `toml:"-"`

	root     *vfs.VFS
	fscancel context.CancelFunc
	vfsopts  vfscommon.Options

	templates      []*template.Template
	serializerFunc telegraf.SerializerFunc
	serializers    map[string]telegraf.Serializer
	modified       map[string]time.Time
	encoder        internal.ContentEncoder

	// mu guards serializers and modified. Both are normally only touched
	// from the single goroutine that calls Write, but WriteContext may
	// abandon a still-running write on context cancellation (see the
	// Contract section of docs/specs/tsd-012-output-context-aware-write.md),
	// so a subsequent Write/WriteContext call's bookkeeping can race with
	// the abandoned goroutine's bookkeeping without this lock.
	mu sync.Mutex

	// newFsFunc creates the underlying rclone backend and defaults to
	// info.NewFs. Overridable in tests to inject a hook that blocks until
	// signaled, so WriteContext/ConnectContext cancellation can be tested
	// deterministically without racing a real hang.
	newFsFunc func(ctx context.Context, name, root string, config configmap.Mapper) (fs.Fs, error)
	// writeFilesFunc performs the actual (potentially blocking) write of
	// already-serialized data to the VFS and defaults to f.writeFiles.
	// Overridable in tests for the same reason as newFsFunc.
	writeFilesFunc func(root *vfs.VFS, groupBuffer map[string][]byte) error
}

func (*File) SampleConfig() string {
	return sampleConfig
}

func (f *File) SetSerializerFunc(sf telegraf.SerializerFunc) {
	f.serializerFunc = sf
}

func (f *File) Init() error {
	if len(f.Files) == 0 {
		return errors.New("no files specified")
	}

	// Set defaults
	if f.Remote.Empty() {
		if err := f.Remote.Set([]byte("local")); err != nil {
			return fmt.Errorf("setting default remote failed: %w", err)
		}
	}

	if f.FinalWriteTimeout <= 0 {
		f.FinalWriteTimeout = config.Duration(10 * time.Second)
	}

	// Prepare VFS options
	f.vfsopts = vfscommon.Opt
	f.vfsopts.CacheMode = vfscommon.CacheModeWrites // required for appends
	if f.WriteBackInterval > 0 {
		f.vfsopts.WriteBack = fs.Duration(f.WriteBackInterval)
	}
	if f.MaxCacheSize > 0 {
		f.vfsopts.CacheMaxSize = fs.SizeSuffix(f.MaxCacheSize)
	}

	// Route rclone's internal logging through the plugin logger
	fs.SetLogger(slog.NewLogger(f.Log).Handler())

	// Setup custom template functions
	funcs := template.FuncMap{"now": time.Now}

	// Setup filename templates
	f.templates = make([]*template.Template, 0, len(f.Files))
	for _, ftmpl := range f.Files {
		tmpl, err := template.New(ftmpl).Funcs(funcs).Parse(ftmpl)
		if err != nil {
			return fmt.Errorf("parsing file template %q failed: %w", ftmpl, err)
		}
		f.templates = append(f.templates, tmpl)
	}

	f.serializers = make(map[string]telegraf.Serializer)
	f.modified = make(map[string]time.Time)

	var options []internal.EncodingOption
	if f.CompressionAlgorithm == "" {
		f.CompressionAlgorithm = "identity"
	}

	if f.CompressionLevel >= 0 {
		options = append(options, internal.WithCompressionLevel(f.CompressionLevel))
	}
	var err error
	f.encoder, err = internal.NewContentEncoder(f.CompressionAlgorithm, options...)

	return err
}

func (f *File) Connect() error {
	return f.ConnectContext(context.Background())
}

// ConnectContext sets up the remote virtual filesystem. It can be
// cancelled via ctx.
//
// rclone's vfs.New explicitly does not derive cancellation from the ctx
// passed to it ("The ctx passed in is not used for cancellation" - see
// vfs.New's doc comment); the context stored inside the VFS/backend
// instead needs to live for as long as the backend does (used for
// background polling/writeback), which is why the existing code already
// derived it from context.Background() rather than any per-call context.
// Whether a given backend's own dial/auth handshake inside NewFs honours
// context cancellation promptly varies by backend (rclone supports many:
// S3, SFTP, local, etc.), so rather than relying on that, the whole setup
// (NewFs + vfs.New + the connectivity-checking List call) is raced in a
// goroutine and abandoned on cancellation per the Contract section of
// docs/specs/tsd-012-output-context-aware-write.md. On abandonment,
// nothing is installed into f - a background goroutine waits for the
// setup to finish (or hang forever, in the pathological case) and shuts
// down any VFS it produced so it isn't leaked; a subsequent Connect/
// ConnectContext call starts its own independent attempt.
func (f *File) ConnectContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	remoteRaw, err := f.Remote.Get()
	if err != nil {
		return fmt.Errorf("getting remote secret failed: %w", err)
	}
	remote := remoteRaw.String()
	remoteRaw.Destroy()

	// Construct the underlying filesystem config
	parsed, err := fspath.Parse(remote)
	if err != nil {
		return fmt.Errorf("parsing remote failed: %w", err)
	}
	info, err := fs.Find(parsed.Name)
	if err != nil {
		return fmt.Errorf("cannot find remote type %q: %w", parsed.Name, err)
	}
	newFsFunc := f.newFsFunc
	if newFsFunc == nil {
		newFsFunc = info.NewFs
	}

	type dialResult struct {
		root *vfs.VFS
		err  error
	}
	done := make(chan dialResult, 1)
	lifetimeCtx, cancel := context.WithCancel(context.Background())
	go func() {
		rootfs, err := newFsFunc(lifetimeCtx, parsed.Name, parsed.Path, fs.ConfigMap(info.Prefix, info.Options, parsed.Name, parsed.Config))
		if err != nil {
			done <- dialResult{err: fmt.Errorf("creating remote failed: %w", err)}
			return
		}
		root := vfs.New(lifetimeCtx, rootfs, &f.vfsopts)

		// Force connection to make sure we actually can connect
		if _, err := root.Fs().List(lifetimeCtx, "/"); err != nil {
			done <- dialResult{err: err}
			return
		}
		done <- dialResult{root: root}
	}()

	select {
	case res := <-done:
		if res.err != nil {
			cancel()
			return res.err
		}
		f.fscancel = cancel
		f.root = res.root
		total, used, free := f.root.Statfs()
		f.Log.Debugf("Connected to %s with %s total, %s used and %s free!",
			f.root.Fs().String(),
			humanize.Bytes(uint64(total)),
			humanize.Bytes(uint64(used)),
			humanize.Bytes(uint64(free)),
		)
		return nil
	case <-ctx.Done():
		// Signal the backend to stop in case it does honour context
		// cancellation, then let setup unwind in the background without
		// installing anything into f. Shut down any VFS it eventually
		// produces so it isn't leaked; a hung backend still leaks the
		// goroutine itself until the underlying call returns.
		cancel()
		go func() {
			if res := <-done; res.err == nil && res.root != nil {
				res.root.Shutdown()
			}
		}()
		return ctx.Err()
	}
}

func (f *File) Close() error {
	// Gracefully shutting down the root VFS
	if f.root != nil {
		f.root.FlushDirCache()
		f.root.WaitForWriters(time.Duration(f.FinalWriteTimeout))
		f.root.Shutdown()
		if err := f.root.CleanUp(); err != nil {
			f.Log.Errorf("Cleaning up vfs failed: %v", err)
		}
		f.root = nil
	}

	if f.fscancel != nil {
		f.fscancel()
		f.fscancel = nil
	}

	return nil
}

func (f *File) Write(metrics []telegraf.Metric) error {
	return f.WriteContext(context.Background(), metrics)
}

// WriteContext serializes the metrics and writes them to the configured
// remote via the VFS. It can be cancelled via ctx.
//
// vfs.Handle (returned by VFS.OpenFile) implements the plain os.File-like
// OsFiler interface - Write/Close take no context, so there is no native
// passthrough at this call site even though rclone's lower-level fs.Fs/
// fs.Object operations do accept one; the VFS layer intentionally hides
// that (its default async write-back, cache_write_back, already decouples
// the actual remote upload from this call in the common case, but
// OpenFile itself can still block, e.g. fetching an existing remote
// object's metadata/content for append mode, or MkdirAll for backends
// without real directories). Per the Contract section of
// docs/specs/tsd-012-output-context-aware-write.md, the actual VFS/remote
// work is therefore raced in a goroutine and abandoned on cancellation.
// f.mu guards the plugin's own bookkeeping (serializers, modified) so an
// abandoned write's bookkeeping can't race a later call's; f.root itself
// is captured into a local before racing so a concurrent Close() setting
// f.root = nil can't race the read. The underlying vfs.VFS is safe for
// concurrent access by design (it backs FUSE mounts, which see concurrent
// syscalls from many processes), so Close() running concurrently with an
// abandoned write is expected to be handled gracefully by the library.
//
// Caveat found during investigation: because rclone fans out to many
// different backend types (S3, SFTP, local, etc.), how promptly an
// individual backend's own network calls actually unblock after
// cancellation - as opposed to how promptly WriteContext returns to its
// caller, which is always prompt - varies by configured backend, and some
// backends may leave a partially-written remote object behind after an
// abandoned write eventually completes or fails.
func (f *File) WriteContext(ctx context.Context, metrics []telegraf.Metric) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var buf bytes.Buffer

	// Group the metrics per output file
	groups := make(map[string][]telegraf.Metric)
	for _, raw := range metrics {
		m := raw
		if wm, ok := raw.(telegraf.UnwrappableMetric); ok {
			m = wm.Unwrap()
		}

		for _, tmpl := range f.templates {
			buf.Reset()
			if err := tmpl.Execute(&buf, m); err != nil {
				f.Log.Errorf("Cannot create filename %q for metric %v: %v", tmpl.Name(), m, err)
				continue
			}
			fn := buf.String()
			groups[fn] = append(groups[fn], m)
		}
	}

	// Serialize the metric groups
	groupBuffer := make(map[string][]byte, len(groups))
	f.mu.Lock()
	for fn, fnMetrics := range groups {
		if _, found := f.serializers[fn]; !found {
			var err error
			if f.serializers[fn], err = f.serializerFunc(); err != nil {
				f.mu.Unlock()
				return fmt.Errorf("creating serializer failed: %w", err)
			}
		}
		serializer := f.serializers[fn]

		if f.UseBatchFormat {
			serialized, err := serializer.SerializeBatch(fnMetrics)
			if err != nil {
				f.Log.Errorf("Could not serialize metrics: %v", err)
				continue
			}
			octets, err := f.encoder.Encode(serialized)
			if err != nil {
				f.Log.Errorf("Could not compress metrics: %v", err)
				continue
			}
			groupBuffer[fn] = octets
		} else {
			for _, m := range fnMetrics {
				serialized, err := serializer.Serialize(m)
				if err != nil {
					f.Log.Errorf("Could not serialize metric: %v", err)
					continue
				}
				octets, err := f.encoder.Encode(serialized)
				if err != nil {
					f.Log.Errorf("Could not compress metric: %v", err)
					continue
				}
				groupBuffer[fn] = append(groupBuffer[fn], octets...)
			}
		}
	}
	f.mu.Unlock()

	writeFilesFunc := f.writeFilesFunc
	if writeFilesFunc == nil {
		writeFilesFunc = f.writeFiles
	}

	done := make(chan error, 1)
	root := f.root
	go func() {
		done <- writeFilesFunc(root, groupBuffer)
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// Let the write finish (or hang forever, in the pathological
		// case) in the background rather than waiting for it; log the
		// eventual outcome instead of dropping it silently.
		go func() {
			if err := <-done; err != nil {
				f.Log.Errorf("Write to remote abandoned after context cancellation eventually failed: %v", err)
			}
		}()
		return ctx.Err()
	}
}

// writeFiles writes the already-serialized per-file buffers to the given
// VFS root, and records what it wrote in f.modified/f.serializers (both
// guarded by f.mu since this may run as an abandoned goroutine racing a
// later call, see WriteContext).
func (f *File) writeFiles(root *vfs.VFS, groupBuffer map[string][]byte) error {
	t := time.Now()
	for fn, serialized := range groupBuffer {
		// Make sure the directory exists
		dir := filepath.Dir(filepath.ToSlash(fn))
		if dir != "." && dir != "/" {
			// Make sure we keep the original path-separators
			if filepath.ToSlash(fn) != fn {
				dir = filepath.FromSlash(dir)
			}
			if err := root.MkdirAll(dir, os.FileMode(root.Opt.DirPerms)); err != nil {
				return fmt.Errorf("creating dir %q failed: %w", dir, err)
			}
		}

		// Open the file for appending or create a new one
		file, err := root.OpenFile(fn, os.O_APPEND|os.O_RDWR|os.O_CREATE, os.FileMode(root.Opt.FilePerms))
		if err != nil {
			return fmt.Errorf("opening file %q: %w", fn, err)
		}

		// Write the data
		if _, err := file.Write(serialized); err != nil {
			file.Close()
			return fmt.Errorf("writing metrics to file %q failed: %w", fn, err)
		}
		file.Close()

		f.mu.Lock()
		f.modified[fn] = t
		f.mu.Unlock()
	}

	// Cleanup internal structures for old files
	if f.ForgetFiles > 0 {
		f.mu.Lock()
		for fn, tmod := range f.modified {
			if t.Sub(tmod) > time.Duration(f.ForgetFiles) {
				delete(f.serializers, fn)
				delete(f.modified, fn)
			}
		}
		f.mu.Unlock()
	}

	return nil
}

func init() {
	outputs.Add("remotefile", func() telegraf.Output {
		return &File{
			CompressionLevel: -1,
		}
	})
}
