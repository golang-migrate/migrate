package source_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/golang-migrate/migrate/v4/source"
	sStub "github.com/golang-migrate/migrate/v4/source/stub"
)

// setGlobalTP installs tp as the global TracerProvider and restores the
// previous provider in t.Cleanup.
func setGlobalTP(t *testing.T, tp *sdktrace.TracerProvider) {
	t.Helper()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
	})
}

// stubMigrations returns a *source.Migrations with a single up+down migration at version 1.
func stubMigrations() *source.Migrations {
	m := source.NewMigrations()
	m.Append(&source.Migration{Version: 1, Direction: source.Up, Identifier: "CREATE 1"})
	m.Append(&source.Migration{Version: 1, Direction: source.Down, Identifier: "DROP 1"})
	return m
}

// newTestDriver installs tp as the global provider and returns an OTelDriver
// wrapping a stub source loaded with one up+down migration at version 1.
func newTestDriver(t *testing.T, tp *sdktrace.TracerProvider) source.Driver {
	t.Helper()
	setGlobalTP(t, tp)

	ctx := context.Background()
	inner, err := (&sStub.Stub{}).Open(ctx, "stub://")
	require.NoError(t, err)
	inner.(*sStub.Stub).Migrations = stubMigrations()
	return source.NewOTelDriver(inner, "stub")
}

func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, len(spans))
	for i, s := range spans {
		out[i] = s.Name()
	}
	return out
}

func findSpan(spans []sdktrace.ReadOnlySpan, name string) (sdktrace.ReadOnlySpan, bool) {
	for _, s := range spans {
		if s.Name() == name {
			return s, true
		}
	}
	return nil, false
}

func TestOTelDriver_Unwrap(t *testing.T) {
	ctx := context.Background()
	inner, err := (&sStub.Stub{}).Open(ctx, "stub://")
	require.NoError(t, err)
	drv := source.NewOTelDriver(inner, "stub")
	assert.Equal(t, inner, drv.(*source.OTelDriver).Unwrap())
}

func TestOTelDriver_ReadUpEmitsSpan(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	drv := newTestDriver(t, tp)
	ctx := context.Background()

	r, _, err := drv.ReadUp(ctx, 1)
	require.NoError(t, err)
	_ = r.Close()

	snaps := exp.GetSpans().Snapshots()
	names := spanNames(snaps)
	assert.Contains(t, names, "source.read_up")
}

func TestOTelDriver_ReadDownEmitsSpan(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	drv := newTestDriver(t, tp)
	ctx := context.Background()

	r, _, err := drv.ReadDown(ctx, 1)
	require.NoError(t, err)
	_ = r.Close()

	snaps := exp.GetSpans().Snapshots()
	names := spanNames(snaps)
	assert.Contains(t, names, "source.read_down")
}

func TestOTelDriver_SpanKindInternal(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	drv := newTestDriver(t, tp)
	ctx := context.Background()

	r, _, err := drv.ReadUp(ctx, 1)
	require.NoError(t, err)
	_ = r.Close()

	snaps := exp.GetSpans().Snapshots()
	span, ok := findSpan(snaps, "source.read_up")
	require.True(t, ok)
	assert.Equal(t, trace.SpanKindInternal, span.SpanKind())
}

func TestOTelDriver_VersionAttribute(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	drv := newTestDriver(t, tp)
	ctx := context.Background()

	r, _, err := drv.ReadUp(ctx, 1)
	require.NoError(t, err)
	_ = r.Close()

	snaps := exp.GetSpans().Snapshots()
	span, ok := findSpan(snaps, "source.read_up")
	require.True(t, ok)

	for _, kv := range span.Attributes() {
		if string(kv.Key) == "migrate.version" {
			assert.Equal(t, int64(1), kv.Value.AsInt64())
			return
		}
	}
	t.Error("migrate.version attribute not found on source.read_up span")
}

func TestOTelDriver_ErrNotExistIsNotErrorSpan(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	drv := newTestDriver(t, tp)
	ctx := context.Background()

	// Version 99 does not exist — ReadUp returns os.ErrNotExist.
	_, _, err := drv.ReadUp(ctx, 99)
	require.Error(t, err, "expected ErrNotExist-wrapped error")

	snaps := exp.GetSpans().Snapshots()
	span, ok := findSpan(snaps, "source.read_up")
	require.True(t, ok)

	// Span status must NOT be Error for os.ErrNotExist.
	assert.NotEqual(t, codes.Error, span.Status().Code,
		"os.ErrNotExist must not produce an error span")
}

func TestOTelDriver_NoSpanForFirstPrevNext(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	drv := newTestDriver(t, tp)
	ctx := context.Background()

	// First, Prev, Next are in-memory — no spans expected.
	_, _ = drv.First(ctx)
	_, _ = drv.Next(ctx, 1)
	_, _ = drv.Prev(ctx, 1)

	names := spanNames(exp.GetSpans().Snapshots())
	assert.NotContains(t, names, "source.first")
	assert.NotContains(t, names, "source.next")
	assert.NotContains(t, names, "source.prev")
	assert.Empty(t, names, "First/Prev/Next must not emit spans")
}

func TestOTelDriver_NoSpanForOpenAndClose(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	drv := newTestDriver(t, tp)
	ctx := context.Background()

	_, _ = drv.Open(ctx, "stub://")
	_ = drv.Close(ctx)

	assert.Empty(t, exp.GetSpans().Snapshots(), "Open and Close must not emit spans")
}
