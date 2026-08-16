//go:generate ../../../tools/readme_config_includer/generator
package iotdb

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/iotdb-client-go/client"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/internal/choice"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

// matches any word that has a non valid backtick
// `word`  							 <- doesn't match
// “word , `wo`rd` , `word , word`   <- match
var forbiddenBacktick = regexp.MustCompile("^[^\x60].*?[\x60]+.*?[^\x60]$|^[\x60].*[\x60]+.*[\x60]$|^[\x60]+.*[^\x60]$|^[^\x60].*[\x60]+$")
var allowedBacktick = regexp.MustCompile("^[\x60].*[\x60]$")

// Allow one replacement attempt while cleanup of the original session is
// stuck, but do not accumulate sessions indefinitely on repeated timeouts.
const (
	maxPoisonedSessions = 2
	defaultCloseTimeout = 5 * time.Second
)

// ioTDBSession is the subset of *client.Session used by this plugin,
// extracted so tests can substitute a controllable fake instead of dialing
// a real IoTDB server.
type ioTDBSession interface {
	Open(enableRPCCompression bool, connectionTimeoutInMs int) error
	Close() error
	InsertRecords(deviceIds []string, measurements [][]string, dataTypes [][]client.TSDataType, values [][]interface{}, timestamps []int64) error
}

type IoTDB struct {
	Host            string          `toml:"host"`
	Port            string          `toml:"port"`
	User            config.Secret   `toml:"user"`
	Password        config.Secret   `toml:"password"`
	Timeout         config.Duration `toml:"timeout"`
	ConvertUint64To string          `toml:"uint64_conversion"`
	TimeStampUnit   string          `toml:"timestamp_precision"`
	TreatTagsAs     string          `toml:"convert_tags_to"`
	SanitizeTags    string          `toml:"sanitize_tag"`
	Log             telegraf.Logger `toml:"-"`

	sanityRegex []*regexp.Regexp

	// sessionFunc constructs the underlying session. It is a field (rather
	// than a direct call to client.NewSession) so tests can substitute a
	// fake that never dials.
	sessionFunc func(cfg *client.Config) ioTDBSession

	sessionMu    sync.Mutex
	session      ioTDBSession
	sessionGen   uint64
	attempt      *sessionAttempt
	poisoned     int
	limitLogged  bool
	closed       bool
	closeTimeout time.Duration
}

type sessionAttempt struct {
	done       chan struct{}
	session    ioTDBSession
	generation uint64
	err        error

	// abandoned is set (under sessionMu) when every caller waiting on this
	// attempt gave up before Open() returned. It tells createSession to
	// release the poisoned-session slot the abandonment took out.
	abandoned bool
}

type recordsWithTags struct {
	// IoTDB Records basic data struct
	DeviceIDList     []string
	MeasurementsList [][]string
	ValuesList       [][]interface{}
	DataTypesList    [][]client.TSDataType
	TimestampList    []int64
	// extra tags
	TagsList [][]*telegraf.Tag
}

func (*IoTDB) SampleConfig() string {
	return sampleConfig
}

// Init is for setup, and validating config.
func (s *IoTDB) Init() error {
	if s.Timeout < 0 {
		return errors.New("negative timeout")
	}
	if !choice.Contains(s.ConvertUint64To, []string{"int64", "int64_clip", "text"}) {
		return fmt.Errorf("unknown 'uint64_conversion' method %q", s.ConvertUint64To)
	}
	if !choice.Contains(s.TimeStampUnit, []string{"second", "millisecond", "microsecond", "nanosecond"}) {
		return fmt.Errorf("unknown 'timestamp_precision' method %q", s.TimeStampUnit)
	}
	if !choice.Contains(s.TreatTagsAs, []string{"fields", "device_id"}) {
		return fmt.Errorf("unknown 'convert_tags_to' method %q", s.TreatTagsAs)
	}

	if s.User.Empty() {
		s.User.Destroy()
		s.User = config.NewSecret([]byte("root"))
	}
	if s.Password.Empty() {
		s.Password.Destroy()
		s.Password = config.NewSecret([]byte("root"))
	}

	switch s.SanitizeTags {
	case "0.13":
		matchUnsupportedCharacter := regexp.MustCompile("[^0-9a-zA-Z_:@#${}\x60]")

		regex := []*regexp.Regexp{matchUnsupportedCharacter}
		s.sanityRegex = append(s.sanityRegex, regex...)

	// from version 1.x.x IoTDB changed the allowed keys in nodes
	case "1.0", "1.1", "1.2", "1.3":
		matchUnsupportedCharacter := regexp.MustCompile("[^0-9a-zA-Z_\x60]")
		matchNumericString := regexp.MustCompile(`^\d+$`)

		regex := []*regexp.Regexp{matchUnsupportedCharacter, matchNumericString}
		s.sanityRegex = append(s.sanityRegex, regex...)
	}

	if s.sessionFunc == nil {
		s.sessionFunc = defaultSessionFunc
	}

	s.Log.Info("Initialization completed.")
	return nil
}

func (s *IoTDB) Connect() error {
	return s.ConnectContext(context.Background())
}

// ConnectContext acquires (creating if necessary) the IoTDB session used
// for writes. See acquireSession for the cancellation-safety approach.
func (s *IoTDB) ConnectContext(ctx context.Context) error {
	_, _, err := s.acquireSession(ctx)
	return err
}

// Close closes the live session, if any. It bounds the wait on the
// underlying (context-less) Close RPC the same way outputs.kafka bounds
// its producer close, so a stuck server can't hang agent shutdown forever.
// This only ever touches the current, non-poisoned session -- poisoned
// sessions from cancelled writes/connects are cleaned up independently, see
// abandonSession.
func (s *IoTDB) Close() error {
	s.sessionMu.Lock()
	s.closed = true
	session := s.session
	s.session = nil
	s.sessionMu.Unlock()
	if session == nil {
		return nil
	}

	done := make(chan error, 1)
	go func() {
		done <- session.Close()
	}()
	timeout := s.sessionCloseTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return fmt.Errorf("closing IoTDB session timed out after %s", timeout)
	}
}

// acquireSession returns the current live session if one exists, or races a
// shared single-flight creation attempt against ctx.Done(), mirroring
// outputs.kafka's acquireProducer/createProducer/disposeProducer pattern.
func (s *IoTDB) acquireSession(ctx context.Context) (ioTDBSession, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	s.sessionMu.Lock()
	if s.closed {
		s.sessionMu.Unlock()
		return nil, 0, errors.New("iotdb output is closed")
	}
	if s.session != nil {
		session, generation := s.session, s.sessionGen
		s.sessionMu.Unlock()
		return session, generation, nil
	}
	if s.poisoned >= maxPoisonedSessions {
		if !s.limitLogged {
			s.Log.Errorf("IoTDB session replacement limit reached; refusing to create another session because previous sessions may still be in use by an abandoned call")
			s.limitLogged = true
		}
		s.sessionMu.Unlock()
		return nil, 0, errors.New("iotdb session replacement limit reached while previous sessions may still be in use")
	}
	if s.sessionFunc == nil {
		s.sessionFunc = defaultSessionFunc
	}

	attempt := s.attempt
	startAttempt := false
	if attempt == nil {
		attempt = &sessionAttempt{done: make(chan struct{})}
		s.attempt = attempt
		startAttempt = true
	}
	s.sessionMu.Unlock()
	if startAttempt {
		go s.createSession(attempt)
	}

	select {
	case <-attempt.done:
		if attempt.err != nil && ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		return attempt.session, attempt.generation, attempt.err
	case <-ctx.Done():
		s.abandonAttempt(attempt)
		return nil, 0, ctx.Err()
	}
}

// abandonAttempt detaches a creation attempt whose caller timed out, so that
// a later Connect starts a fresh one instead of joining a Session.Open() that
// may never return. Without this a single permanently hung Open() would wedge
// every subsequent connection attempt for the lifetime of the process.
//
// The still-running Open() cannot be cancelled, so the abandoned attempt takes
// out a poisoned-session slot for exactly as long as it runs; that is what
// bounds how many hung Open() goroutines can accumulate.
func (s *IoTDB) abandonAttempt(attempt *sessionAttempt) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	// createSession clears s.attempt under the same lock once it finishes, so
	// a mismatch here means the attempt already completed and there is
	// nothing to abandon.
	if s.attempt != attempt {
		return
	}
	s.attempt = nil
	attempt.abandoned = true
	s.poisoned++

	s.Log.Warnf("Abandoning IoTDB session creation after cancellation; the underlying Open() may still be " +
		"running and its session will be closed once it returns")
}

// createSession runs Open() in the background on behalf of whichever caller
// started the single-flight attempt; callers only bound their own wait via
// acquireSession's select. If the result is no longer wanted by the time it
// completes (closed, or superseded by a newer attempt), this goroutine is
// the session's sole owner up to this point, so it is safe for it to close
// the session itself.
func (s *IoTDB) createSession(attempt *sessionAttempt) {
	session, err := s.newSession()

	s.sessionMu.Lock()
	// An abandoned attempt (see abandonAttempt) is no longer s.attempt, but a
	// session it eventually produces is still perfectly good: install it as
	// long as nothing else got there first, so a merely slow Open() is not
	// wasted. Only a genuinely superseded result is discarded.
	install := err == nil && !s.closed && s.session == nil
	if install {
		s.session = session
		s.sessionGen++
		attempt.session = session
		attempt.generation = s.sessionGen
	} else if err != nil {
		attempt.err = &internal.StartupError{Err: err, Retry: true}
	} else {
		attempt.err = errors.New("iotdb session creation result is no longer usable")
	}
	if s.attempt == attempt {
		s.attempt = nil
	}
	if attempt.abandoned {
		// The hung Open() has returned, so release the slot it was holding.
		s.poisoned--
		s.limitLogged = false
	}
	close(attempt.done)
	s.sessionMu.Unlock()

	if session != nil && !install {
		s.disposeSession(session)
	}
}

func (s *IoTDB) newSession() (ioTDBSession, error) {
	username, err := s.User.Get()
	if err != nil {
		return nil, fmt.Errorf("getting username failed: %w", err)
	}
	password, err := s.Password.Get()
	if err != nil {
		username.Destroy()
		return nil, fmt.Errorf("getting password failed: %w", err)
	}
	cfg := &client.Config{
		Host:     s.Host,
		Port:     s.Port,
		UserName: username.String(),
		Password: password.String(),
	}
	username.Destroy()
	password.Destroy()

	session := s.sessionFunc(cfg)
	timeoutInMs := int(time.Duration(s.Timeout).Milliseconds())
	if err := session.Open(false, timeoutInMs); err != nil {
		return nil, fmt.Errorf("connecting to %s:%s failed: %w", s.Host, s.Port, err)
	}
	return session, nil
}

// disposeSession closes a session nobody wants, bounding the wait with a
// timer purely for logging purposes -- like outputs.kafka's
// disposeProducer, it does not force anything, it just stops waiting. This
// is safe because the calling goroutine (createSession) has been this
// session's only accessor for its entire life; it was never installed or
// handed to a writer.
func (s *IoTDB) disposeSession(session ioTDBSession) {
	done := make(chan error, 1)
	go func() {
		done <- session.Close()
	}()
	timer := time.NewTimer(s.sessionCloseTimeout())
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			s.Log.Errorf("Error closing unused IoTDB session: %v", err)
		}
	case <-timer.C:
		s.Log.Errorf("Timed out closing unused IoTDB session; abandoning it")
	}
}

func (s *IoTDB) sessionCloseTimeout() time.Duration {
	if s.closeTimeout > 0 {
		return s.closeTimeout
	}
	return defaultCloseTimeout
}

// Write should write immediately to the output, and not buffer writes
// (Telegraf manages the buffer for you). Returning an error will fail this
// batch of writes and the entire batch will be retried automatically.
func (s *IoTDB) Write(metrics []telegraf.Metric) error {
	return s.WriteContext(context.Background(), metrics)
}

// WriteContext writes the metrics to IoTDB. It can be cancelled via the
// context. The underlying Thrift session has no context support and, unlike
// outputs.kafka/outputs.nsq's clients, is not safe to close concurrently
// with an in-flight call (see abandonSession for why), so on cancellation
// the session is merely detached and handed off to the still-running
// goroutine to close once it naturally returns -- see abandonSession.
func (s *IoTDB) WriteContext(ctx context.Context, metrics []telegraf.Metric) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Convert Metrics to Records with Tags
	rwt, err := s.convertMetricsToRecordsWithTags(metrics)
	if err != nil {
		return err
	}
	if err := s.modifyRecordsWithTags(rwt); err != nil {
		return err
	}

	session, generation, err := s.acquireSession(ctx)
	if err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() {
		// If first writing fails, the client will automatically retry
		// three times. If all fail, it returns an error.
		done <- session.InsertRecords(
			rwt.DeviceIDList,
			rwt.MeasurementsList,
			rwt.DataTypesList,
			rwt.ValuesList,
			rwt.TimestampList,
		)
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("write failed: %w", err)
		}
		return nil
	case <-ctx.Done():
		s.abandonSession(session, generation, done)
		return ctx.Err()
	}
}

// abandonSession detaches a session whose in-flight InsertRecords call was
// cancelled.
//
// Unlike outputs.kafka's sarama SyncProducer or outputs.nsq's go-nsq
// Producer, IoTDB's generated Thrift client (client.Session) has no
// internal locking: its fields (transport, RPC client, session/statement
// IDs) are plain, unguarded struct fields, and Session.Close itself issues
// another RPC (CloseSession) over the same shared transport before closing
// it. Calling Close concurrently with an in-flight InsertRecords call on
// the same Session would interleave two requests on one Thrift transport
// with no synchronization protecting it, which can corrupt the wire
// framing for both calls or race on the unguarded fields -- this is not a
// safe abandon-and-close-concurrently pattern the way it is for kafka/nsq.
//
// So this does NOT close the session here. Instead, it hands ownership to
// the goroutine already blocked inside the call: once that call returns
// (successfully, with an error, or -- in the worst case -- never, if the
// peer is truly gone), that goroutine is once again the *sole* accessor of
// the session, and it is only then safe for it to close it. This bounds the
// leak to "one extra goroutine and TCP connection until the original
// blocked call unblocks on its own", never eliminates it -- if the peer
// never responds and never resets the connection, the abandoned session's
// resources are never reclaimed. This residual risk is why the number of
// concurrently poisoned sessions is capped (maxPoisonedSessions) and why it
// is documented here and in the README rather than presented as fully
// bounded the way outputs.kafka's producer replacement is.
func (s *IoTDB) abandonSession(session ioTDBSession, generation uint64, callDone <-chan error) {
	s.sessionMu.Lock()
	if s.sessionGen != generation || s.session == nil {
		s.sessionMu.Unlock()
		return
	}
	s.session = nil
	s.poisoned++
	s.sessionMu.Unlock()

	s.Log.Warnf("Abandoning IoTDB session after write cancellation; the underlying call may still be running against the stale session and will be closed once it returns")

	go func() {
		// Wait for the original, still in-flight call to finish on its own
		// before touching the session -- see the abandonSession doc
		// comment for why this cannot be raced like kafka/nsq.
		<-callDone
		if err := session.Close(); err != nil {
			s.Log.Errorf("Error closing abandoned IoTDB session: %v", err)
		}
		s.sessionMu.Lock()
		s.poisoned--
		s.limitLogged = false
		s.sessionMu.Unlock()
	}()
}

// Find out data type of the value and return it's id in TSDataType, and convert it if necessary.
func (s *IoTDB) getDataTypeAndValue(value interface{}) (client.TSDataType, interface{}) {
	switch v := value.(type) {
	case int32:
		return client.INT32, v
	case int64:
		return client.INT64, v
	case uint32:
		return client.INT64, int64(v)
	case uint64:
		switch s.ConvertUint64To {
		case "int64_clip":
			if v <= uint64(math.MaxInt64) {
				return client.INT64, int64(v)
			}
			return client.INT64, int64(math.MaxInt64)
		case "int64":
			return client.INT64, int64(v)
		case "text":
			return client.TEXT, strconv.FormatUint(v, 10)
		default:
			return client.UNKNOWN, int64(0)
		}
	case float64:
		return client.DOUBLE, v
	case string:
		return client.TEXT, v
	case bool:
		return client.BOOLEAN, v
	default:
		return client.UNKNOWN, int64(0)
	}
}

// convert Timestamp Unit according to config
func (s *IoTDB) convertTimestampOfMetric(m telegraf.Metric) (int64, error) {
	switch s.TimeStampUnit {
	case "second":
		return m.Time().Unix(), nil
	case "millisecond":
		return m.Time().UnixMilli(), nil
	case "microsecond":
		return m.Time().UnixMicro(), nil
	case "nanosecond":
		return m.Time().UnixNano(), nil
	default:
		return 0, fmt.Errorf("unknown timestamp_precision %q", s.TimeStampUnit)
	}
}

// convert Metrics to Records with tags
func (s *IoTDB) convertMetricsToRecordsWithTags(metrics []telegraf.Metric) (*recordsWithTags, error) {
	timestampList := make([]int64, 0, len(metrics))
	deviceidList := make([]string, 0, len(metrics))
	measurementsList := make([][]string, 0, len(metrics))
	valuesList := make([][]interface{}, 0, len(metrics))
	dataTypesList := make([][]client.TSDataType, 0, len(metrics))
	tagsList := make([][]*telegraf.Tag, 0, len(metrics))

	for _, metric := range metrics {
		// write `metric` to the output sink here
		// deal with basic parameter
		keys := make([]string, 0, len(metric.FieldList()))
		values := make([]interface{}, 0, len(metric.FieldList()))
		dataTypes := make([]client.TSDataType, 0, len(metric.FieldList()))
		for _, field := range metric.FieldList() {
			datatype, value := s.getDataTypeAndValue(field.Value)
			if datatype == client.UNKNOWN {
				return nil, fmt.Errorf("datatype of %q is unknown, values: %v", field.Key, field.Value)
			}
			keys = append(keys, field.Key)
			values = append(values, value)
			dataTypes = append(dataTypes, datatype)
		}
		// Convert timestamp into specified unit
		ts, err := s.convertTimestampOfMetric(metric)
		if err != nil {
			return nil, err
		}
		timestampList = append(timestampList, ts)
		// append all metric data of this record to lists
		deviceidList = append(deviceidList, metric.Name())
		measurementsList = append(measurementsList, keys)
		valuesList = append(valuesList, values)
		dataTypesList = append(dataTypesList, dataTypes)
		tagsList = append(tagsList, metric.TagList())
	}
	rwt := &recordsWithTags{
		DeviceIDList:     deviceidList,
		MeasurementsList: measurementsList,
		ValuesList:       valuesList,
		DataTypesList:    dataTypesList,
		TimestampList:    timestampList,
		TagsList:         tagsList,
	}
	return rwt, nil
}

// checks is the tag contains any IoTDB invalid character
func (s *IoTDB) validateTag(tag string) (string, error) {
	// IoTDB uses "root" as a keyword and can be called only at the start of the path
	if tag == "root" {
		return "", errors.New("cannot use 'root' as tag")
	} else if forbiddenBacktick.MatchString(tag) { // returns an error if the backsticks are used in an inappropriate way
		return "", errors.New("cannot use ` in tag names")
	} else if allowedBacktick.MatchString(tag) { // if the tag in already enclosed in tags returns the tag
		return tag, nil
	}

	// loops through all the regex patterns and if one
	// pattern matches returns the tag between `
	for _, regex := range s.sanityRegex {
		if regex.MatchString(tag) {
			return "`" + tag + "`", nil
		}
	}

	return tag, nil
}

// modify recordsWithTags according to 'TreatTagsAs' Configuration
func (s *IoTDB) modifyRecordsWithTags(rwt *recordsWithTags) error {
	switch s.TreatTagsAs {
	case "fields":
		// method 1: treat Tag(Key:Value) as measurement
		for index, tags := range rwt.TagsList { // for each record
			for _, tag := range tags { // for each tag of this record, append it's Key:Value to measurements
				datatype, value := s.getDataTypeAndValue(tag.Value)
				if datatype == client.UNKNOWN {
					return fmt.Errorf("datatype of %q is unknown, values: %v", tag.Key, value)
				}
				rwt.MeasurementsList[index] = append(rwt.MeasurementsList[index], tag.Key)
				rwt.ValuesList[index] = append(rwt.ValuesList[index], value)
				rwt.DataTypesList[index] = append(rwt.DataTypesList[index], datatype)
			}
		}
		return nil
	case "device_id":
		// method 2: treat Tag(Key:Value) as subtree of device id
		for index, tags := range rwt.TagsList { // for each record
			topic := make([]string, 0, len(tags)+1)
			topic = append(topic, rwt.DeviceIDList[index])
			for _, tag := range tags { // for each tag, append it's Value
				tagValue, err := s.validateTag(tag.Value) // validates tag
				if err != nil {
					return err
				}
				topic = append(topic, tagValue)
			}
			rwt.DeviceIDList[index] = strings.Join(topic, ".")
		}
		return nil
	default:
		// something go wrong. This configuration should have been checked in func Init().
		return fmt.Errorf("unknown 'convert_tags_to' method: %q", s.TreatTagsAs)
	}
}

func defaultSessionFunc(cfg *client.Config) ioTDBSession {
	session := client.NewSession(cfg)
	return &session
}

func init() {
	outputs.Add("iotdb", func() telegraf.Output { return newIoTDB() })
}

// create a new IoTDB struct with default values.
func newIoTDB() *IoTDB {
	return &IoTDB{
		Host:            "localhost",
		Port:            "6667",
		Timeout:         config.Duration(time.Second * 5),
		ConvertUint64To: "int64_clip",
		TimeStampUnit:   "nanosecond",
		TreatTagsAs:     "device_id",
		sessionFunc:     defaultSessionFunc,
	}
}
