package migrate

import (
	"context"
	"fmt"
	"log/slog"
)

// Logger is an interface so you can pass in your own
// logging implementation.
type Logger interface {
	// Printf is like fmt.Printf
	Printf(format string, v ...interface{})

	// Verbose should return true when verbose logging output is wanted
	Verbose() bool
}

// StructuredLogger is an optional capability a [Logger] may also implement to
// receive structured records instead of preformatted Printf strings. A [Logger]
// set as Migrate.Log that implements StructuredLogger has its Log method called
// directly; a plain Printf-only [Logger] is wrapped internally so it still
// receives the historical Printf lines. This lets a structured backend opt in
// without breaking the legacy [Logger] interface.
//
// The args carry no logging-library types, so any backend can be adapted to
// Log: an [*slog.Logger] fits directly (see [SlogLogger]), while others need a
// small adapter (e.g. zap's SugaredLogger.Logw takes the same key/value pairs;
// zerolog needs a per-field conversion).
//
// The msg passed to Log is a stable, human-readable label, but the exact
// strings are not part of the API contract: implementations should log the
// key/value args and treat msg as a description, not switch on it.
type StructuredLogger interface {
	Logger

	// Log emits a single record. args are alternating key/value pairs in the
	// same shape as slog.Logger.Log's args (e.g. "version", 4, "took", d).
	Log(ctx context.Context, level slog.Level, msg string, args ...interface{})
}

// Message strings passed to [StructuredLogger.Log]. They double as the record
// message on the structured path and as the switch key [printfLogger] uses to
// rebuild the historical Printf line, so the two must stay in sync.
const (
	// msgClosing is logged when the source and database drivers are closed.
	msgClosing = "closing source and database"
	// msgStartBuffering is logged when a migration starts being prefetched.
	msgStartBuffering = "start buffering migration"
	// msgScheduled is logged when a migration is queued without prefetching.
	msgScheduled = "scheduled migration"
	// msgReadExecute is logged just before a migration's body is run.
	msgReadExecute = "read and execute migration"
	// msgApplied is logged once a migration has been applied, with its timing.
	msgApplied = "applied migration"
	// msgError is logged for a migration error, carrying the "error" field.
	msgError = "migration error"
)

// SlogLogger adapts an [*slog.Logger] to the migrate [Logger] and [StructuredLogger]
// interfaces. Verbose lines are logged at [slog.LevelDebug], normal lines at
// [slog.LevelInfo], and errors at [slog.LevelError]; the underlying [slog.Handler]'s
// level decides what is actually emitted. Verbose always reports true so the
// Debug records reach the handler and level filtering happens there rather than
// in migrate.
type SlogLogger struct {
	logger *slog.Logger
}

// SlogLogger must satisfy both the legacy [Logger] and the [StructuredLogger]
// interface. These assignments fail to compile if that ever regresses.
var (
	_ Logger           = (*SlogLogger)(nil)
	_ StructuredLogger = (*SlogLogger)(nil)
)

// NewSlogLogger returns a [SlogLogger] writing to logger. A nil logger falls back
// to [slog.Default] so the adapter is always safe to use.
func NewSlogLogger(logger *slog.Logger) *SlogLogger {
	if logger == nil {
		logger = slog.Default()
	}

	return &SlogLogger{logger: logger}
}

// Log emits a structured record at level.
func (s *SlogLogger) Log(ctx context.Context, level slog.Level, msg string, args ...interface{}) {
	s.logger.Log(ctx, level, msg, args...)
}

// Printf satisfies the legacy [Logger] interface for callers that pass a
// [SlogLogger] where a plain [Logger] is expected. The formatted message is
// logged at [slog.LevelInfo], except messages prefixed with "error: " which are
// logged at [slog.LevelError] to match migrate's own error convention.
func (s *SlogLogger) Printf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)

	level := slog.LevelInfo
	if len(msg) >= len(errPrefix) && msg[:len(errPrefix)] == errPrefix {
		level = slog.LevelError
	}

	s.logger.Log(context.Background(), level, msg)
}

// Verbose reports true so verbose Debug records reach the handler; the handler's
// own level then decides whether they are emitted.
func (s *SlogLogger) Verbose() bool { return true }

// errPrefix marks Printf messages that carry an error. [SlogLogger.Printf] uses
// it to route such messages to [slog.LevelError], matching migrate's convention
// of prefixing error lines with "error: ".
const errPrefix = "error: "

// printfLogger wraps a plain Printf-only [Logger] and satisfies StructuredLogger
// by rebuilding the historical Printf lines from each record's message and args.
// It is the compatibility shim: it owns the mapping from structured message to
// legacy format so that call sites only ever emit structured records.
type printfLogger struct{ Logger }

// printfLogger must satisfy [StructuredLogger] too: it is how a plain Printf-only
// [Logger] gets the structured Log method.
var _ StructuredLogger = printfLogger{}

// arg returns the value stored under key in the alternating key/value args, or
// nil when the key is absent.
func arg(key string, args []interface{}) interface{} {
	for i := 0; i+1 < len(args); i += 2 {
		if k, ok := args[i].(string); ok && k == key {
			return args[i+1]
		}
	}

	return nil
}

// Log turns a structured record back into the exact Printf line migrate emitted
// before structured logging existed. Verbose (Debug) lines are suppressed unless
// the wrapped [Logger] asks for verbose output; the applied-migration line also
// gains its read/ran breakdown only when verbose.
func (p printfLogger) Log(_ context.Context, level slog.Level, msg string, args ...interface{}) {
	if level == slog.LevelDebug && !p.Verbose() {
		return
	}

	v, d, id := arg("version", args), arg("direction", args), arg("identifier", args)

	switch msg {
	case msgClosing:
		p.Printf("Closing source and database\n")
	case msgStartBuffering:
		p.Printf("Start buffering %v/%v %v\n", v, d, id)
	case msgScheduled:
		p.Printf("Scheduled %v/%v %v\n", v, d, id)
	case msgReadExecute:
		p.Printf("Read and execute %v/%v %v\n", v, d, id)
	case msgApplied:
		if p.Verbose() {
			p.Printf("Finished %v/%v %v (read %v, ran %v)\n", v, d, id, arg("read", args), arg("ran", args))
		} else {
			p.Printf("%v/%v %v (%v)\n", v, d, id, arg("took", args))
		}
	case msgError:
		p.Printf(errPrefix+"%v", arg("error", args))
	default:
		p.Printf("%s", msg)
	}
}
