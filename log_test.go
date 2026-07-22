package migrate

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	dStub "github.com/golang-migrate/migrate/v4/database/stub"
	sStub "github.com/golang-migrate/migrate/v4/source/stub"
)

// captureHandler is a [slog.Handler] that records the records it receives so
// tests can assert on level, message and attributes.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
	level   slog.Level
}

func (h *captureHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.records = append(h.records, r.Clone())

	return nil
}

func (h *captureHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(_ string) slog.Handler      { return h }

func (h *captureHandler) snapshot() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]slog.Record(nil), h.records...)
}

// attr returns the value of the named attribute on r, or nil if absent.
func attr(r slog.Record, key string) any {
	var out any

	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			out = a.Value.Any()
			return false
		}

		return true
	})

	return out
}

func TestSlogLoggerLog(t *testing.T) {
	h := &captureHandler{level: slog.LevelDebug}
	l := NewSlogLogger(slog.New(h))

	l.Log(context.Background(), slog.LevelInfo, "applied migration", "version", uint(4), "direction", "u")

	recs := h.snapshot()
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}

	if recs[0].Level != slog.LevelInfo {
		t.Errorf("level = %v, want Info", recs[0].Level)
	}

	if recs[0].Message != "applied migration" {
		t.Errorf("message = %q, want %q", recs[0].Message, "applied migration")
	}
	// slog normalizes an unsigned integer to KindUint64, so Value.Any()
	// returns uint64 regardless of the caller's concrete unsigned type.
	if got := attr(recs[0], "version"); got != uint64(4) {
		t.Errorf("version attr = %v (%T), want uint64(4)", got, got)
	}

	if got := attr(recs[0], "direction"); got != "u" {
		t.Errorf("direction attr = %v, want u", got)
	}
}

func TestSlogLoggerPrintfLevel(t *testing.T) {
	tests := []struct {
		name  string
		msg   string
		level slog.Level
	}{
		{"normal", "1/u CREATE 1 (1ms)", slog.LevelInfo},
		{"error prefixed", "error: boom", slog.LevelError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &captureHandler{level: slog.LevelDebug}
			l := NewSlogLogger(slog.New(h))

			l.Printf("%s", tt.msg)

			recs := h.snapshot()
			if len(recs) != 1 {
				t.Fatalf("expected 1 record, got %d", len(recs))
			}

			if recs[0].Level != tt.level {
				t.Errorf("level = %v, want %v", recs[0].Level, tt.level)
			}

			if recs[0].Message != tt.msg {
				t.Errorf("message = %q, want %q", recs[0].Message, tt.msg)
			}
		})
	}
}

func TestSlogLoggerVerbose(t *testing.T) {
	// Verbose must report true so verbose Debug records reach the handler and
	// level filtering happens in slog, not in migrate.
	if !NewSlogLogger(slog.Default()).Verbose() {
		t.Error("SlogLogger.Verbose() = false, want true")
	}
}

func TestNewSlogLoggerNilFallsBackToDefault(t *testing.T) {
	if NewSlogLogger(nil).logger != slog.Default() {
		t.Error("NewSlogLogger(nil) should fall back to slog.Default()")
	}
}

// TestMigrateStructuredLogger runs a real migration and asserts that a
// structured-capable logger ([SlogLogger]) receives an "applied migration" record
// with structured timing fields rather than a preformatted string.
func TestMigrateStructuredLogger(t *testing.T) {
	m, _ := New("stub://", "stub://")
	m.sourceDrv.(*sStub.Stub).Migrations = sourceStubMigrations
	_ = m.databaseDrv.(*dStub.Stub)

	h := &captureHandler{level: slog.LevelDebug}
	m.Log = NewSlogLogger(slog.New(h))

	if err := m.Migrate(1); err != nil {
		t.Fatal(err)
	}

	var applied *slog.Record

	for _, r := range h.snapshot() {
		if r.Message == "applied migration" {
			rr := r
			applied = &rr

			break
		}
	}

	if applied == nil {
		t.Fatal("no \"applied migration\" record was emitted")
	}

	if applied.Level != slog.LevelInfo {
		t.Errorf("level = %v, want Info", applied.Level)
	}

	if got := attr(*applied, "version"); got != uint64(1) {
		t.Errorf("version attr = %v (%T), want uint64(1)", got, got)
	}

	if got := attr(*applied, "direction"); got != "u" {
		t.Errorf("direction attr = %v, want u", got)
	}

	if attr(*applied, "took") == nil {
		t.Error("expected a \"took\" duration attr")
	}
}

// TestMigrateLegacyLogger confirms that a plain Printf-only [Logger] still
// receives the historical preformatted output, unchanged by the structured path.
//
// migrate logs from multiple goroutines (Run buffers migrations concurrently),
// so recordingLogger guards its buffer with a mutex — a real Logger must be
// safe for concurrent use, and an unsynchronized buffer races and drops lines.
type recordingLogger struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	verbose bool
}

func (l *recordingLogger) Printf(format string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf.WriteString(strings.TrimRight(fmt.Sprintf(format, v...), "\n"))
	l.buf.WriteString("\n")
}

func (l *recordingLogger) Verbose() bool { return l.verbose }

// output returns the accumulated log text under the lock.
func (l *recordingLogger) output() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func TestMigrateLegacyLogger(t *testing.T) {
	m, _ := New("stub://", "stub://")
	m.sourceDrv.(*sStub.Stub).Migrations = sourceStubMigrations
	_ = m.databaseDrv.(*dStub.Stub)

	lg := &recordingLogger{}
	m.Log = lg

	if err := m.Migrate(1); err != nil {
		t.Fatal(err)
	}

	out := lg.output()
	// Non-verbose applied line: "<version>/<dir> <identifier> (<took>)".
	if !strings.Contains(out, "1/u 1.up.stub (") {
		t.Errorf("legacy output missing applied line, got:\n%s", out)
	}
}

// TestPrintfLoggerLog checks the compatibility shim rebuilds the historical
// Printf lines from structured records, including the verbose gate.
func TestPrintfLoggerLog(t *testing.T) {
	migArgs := []any{"version", uint(4), "direction", "u", "identifier", "widgets"}

	tests := []struct {
		name    string
		verbose bool
		level   slog.Level
		msg     string
		args    []any
		want    string // "" means nothing should be logged
	}{
		{"scheduled non-verbose suppressed", false, slog.LevelDebug, msgScheduled, migArgs, ""},
		{"scheduled verbose", true, slog.LevelDebug, msgScheduled, migArgs, "Scheduled 4/u widgets"},
		{"start buffering verbose", true, slog.LevelDebug, msgStartBuffering, migArgs, "Start buffering 4/u widgets"},
		{"read and execute verbose", true, slog.LevelDebug, msgReadExecute, migArgs, "Read and execute 4/u widgets"},
		{"closing verbose", true, slog.LevelDebug, msgClosing, nil, "Closing source and database"},
		{
			"applied normal", false, slog.LevelInfo, msgApplied,
			append(append([]any{}, migArgs...), "read", "1ms", "ran", "2ms", "took", "3ms"),
			"4/u widgets (3ms)",
		},
		{
			"applied verbose", true, slog.LevelInfo, msgApplied,
			append(append([]any{}, migArgs...), "read", "1ms", "ran", "2ms", "took", "3ms"),
			"Finished 4/u widgets (read 1ms, ran 2ms)",
		},
		{"error", false, slog.LevelError, msgError, []any{"error", "boom"}, "error: boom"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lg := &recordingLogger{verbose: tt.verbose}
			printfLogger{lg}.Log(context.Background(), tt.level, tt.msg, tt.args...)

			got := strings.TrimRight(lg.output(), "\n")
			if got != tt.want {
				t.Errorf("Log() = %q, want %q", got, tt.want)
			}
		})
	}
}

// legacyDurationRe matches the per-run durations in the applied-migration
// lines so the golden compares formats, not timing.
var legacyDurationRe = regexp.MustCompile(`(read |ran |\()[0-9.]+(µs|ns|ms|s)`)

// goldenLegacyLogNonVerbose and goldenLegacyLogVerbose are the exact
// (duration-redacted) Printf lines a plain [Logger] received from migrate at
// commit 8a4b4bb, before structured logging existed. They were captured by
// running a full Up+Down of sourceStubMigrations against that commit. migrate
// buffers migrations concurrently, so line ORDER is nondeterministic (the base
// commit varies run to run); the assertion below therefore compares the sorted
// multiset of lines, which is stable and proves byte-identity of every line.
var goldenLegacyLogNonVerbose = []string{
	"1/d 1.down.stub (<DUR>)",
	"1/u 1.up.stub (<DUR>)",
	"3/d <empty> (<DUR>)",
	"3/u 3.up.stub (<DUR>)",
	"4/d 4.down.stub (<DUR>)",
	"4/u 4.up.stub (<DUR>)",
	"5/d 5.down.stub (<DUR>)",
	"5/u <empty> (<DUR>)",
	"7/d 7.down.stub (<DUR>)",
	"7/u 7.up.stub (<DUR>)",
}

var goldenLegacyLogVerbose = []string{
	"Closing source and database",
	"Finished 1/d 1.down.stub (read <DUR>, ran <DUR>)",
	"Finished 1/u 1.up.stub (read <DUR>, ran <DUR>)",
	"Finished 3/d <empty> (read <DUR>, ran <DUR>)",
	"Finished 3/u 3.up.stub (read <DUR>, ran <DUR>)",
	"Finished 4/d 4.down.stub (read <DUR>, ran <DUR>)",
	"Finished 4/u 4.up.stub (read <DUR>, ran <DUR>)",
	"Finished 5/d 5.down.stub (read <DUR>, ran <DUR>)",
	"Finished 5/u <empty> (read <DUR>, ran <DUR>)",
	"Finished 7/d 7.down.stub (read <DUR>, ran <DUR>)",
	"Finished 7/u 7.up.stub (read <DUR>, ran <DUR>)",
	"Read and execute 1/d 1.down.stub",
	"Read and execute 1/u 1.up.stub",
	"Read and execute 3/u 3.up.stub",
	"Read and execute 4/d 4.down.stub",
	"Read and execute 4/u 4.up.stub",
	"Read and execute 5/d 5.down.stub",
	"Read and execute 7/d 7.down.stub",
	"Read and execute 7/u 7.up.stub",
	"Scheduled 3/d <empty>",
	"Scheduled 5/u <empty>",
	"Start buffering 1/d 1.down.stub",
	"Start buffering 1/u 1.up.stub",
	"Start buffering 3/u 3.up.stub",
	"Start buffering 4/d 4.down.stub",
	"Start buffering 4/u 4.up.stub",
	"Start buffering 5/d 5.down.stub",
	"Start buffering 7/d 7.down.stub",
	"Start buffering 7/u 7.up.stub",
}

// captureLegacyLog runs a full Up+Down through [printfLogger] and returns the
// emitted lines, duration-redacted and sorted.
func captureLegacyLog(t *testing.T, verbose bool) []string {
	t.Helper()

	m, err := New("stub://", "stub://")
	if err != nil {
		t.Fatal(err)
	}

	m.sourceDrv.(*sStub.Stub).Migrations = sourceStubMigrations
	_ = m.databaseDrv.(*dStub.Stub)

	lg := &recordingLogger{verbose: verbose}
	m.Log = lg

	if err := m.Up(); err != nil {
		t.Fatalf("up: %v", err)
	}

	if err := m.Down(); err != nil {
		t.Fatalf("down: %v", err)
	}

	if s, d := m.Close(); s != nil || d != nil {
		t.Fatalf("close: %v %v", s, d)
	}

	var lines []string

	for ln := range strings.SplitSeq(strings.TrimRight(lg.output(), "\n"), "\n") {
		lines = append(lines, legacyDurationRe.ReplaceAllString(ln, "${1}<DUR>"))
	}

	sort.Strings(lines)

	return lines
}

// TestLegacyLogByteIdentical proves the compatibility shim reproduces the
// pre-structured-logging Printf output byte-for-byte: the lines a plain [Logger]
// receives now must equal the golden captured from the base commit.
func TestLegacyLogByteIdentical(t *testing.T) {
	tests := []struct {
		verbose bool
		golden  []string
	}{
		{false, goldenLegacyLogNonVerbose},
		{true, goldenLegacyLogVerbose},
	}

	for _, tt := range tests {
		name := "non-verbose"
		if tt.verbose {
			name = "verbose"
		}

		t.Run(name, func(t *testing.T) {
			got := captureLegacyLog(t, tt.verbose)

			want := append([]string(nil), tt.golden...)
			sort.Strings(want)

			if len(got) != len(want) {
				t.Fatalf("got %d lines, want %d\ngot:\n%s\nwant:\n%s",
					len(got), len(want), strings.Join(got, "\n"), strings.Join(want, "\n"))
			}

			for i := range want {
				if got[i] != want[i] {
					t.Errorf("line %d = %q, want %q", i, got[i], want[i])
				}
			}
		})
	}
}
