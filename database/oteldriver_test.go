package database_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/golang-migrate/migrate/v4/database"
	dStub "github.com/golang-migrate/migrate/v4/database/stub"
)

// setGlobalTP sets tp as the global TracerProvider for the duration of the
// test and restores the previous provider in t.Cleanup.
func setGlobalTP(t *testing.T, tp *sdktrace.TracerProvider) {
	t.Helper()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
	})
}

// newTestDriver installs tp as the global provider, then returns an OTelDriver
// wrapping a fresh in-memory stub. The inner *dStub.Stub is also returned for
// state inspection.
func newTestDriver(t *testing.T, tp *sdktrace.TracerProvider) database.Driver {
	t.Helper()
	setGlobalTP(t, tp)

	ctx := context.Background()
	inner, err := (&dStub.Stub{}).Open(ctx, "stub://")
	require.NoError(t, err)

	return database.NewOTelDriver(inner, "testdb")
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

func attrVal(span sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsString(), true
		}
	}
	return "", false
}

func TestOTelDriver_Unwrap(t *testing.T) {
	ctx := context.Background()
	inner, err := (&dStub.Stub{}).Open(ctx, "stub://")
	require.NoError(t, err)
	drv := database.NewOTelDriver(inner, "stub")
	assert.Equal(t, inner, drv.(*database.OTelDriver).Unwrap())
}

func TestOTelDriver_SpanNamesAndKind(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	drv := newTestDriver(t, tp)
	ctx := context.Background()

	require.NoError(t, drv.Lock(ctx))
	require.NoError(t, drv.Unlock(ctx))
	_, _, err := drv.Version(ctx)
	require.NoError(t, err)
	require.NoError(t, drv.SetVersion(ctx, 1, true))
	require.NoError(t, drv.SetVersion(ctx, 1, false))
	require.NoError(t, drv.Run(ctx, strings.NewReader("sql")))
	require.NoError(t, drv.Drop(ctx))

	snaps := exp.GetSpans().Snapshots()
	names := spanNames(snaps)

	assert.Contains(t, names, "db.lock")
	assert.Contains(t, names, "db.unlock")
	assert.Contains(t, names, "db.version")
	assert.Contains(t, names, "db.set_version")
	assert.Contains(t, names, "db.run")
	assert.Contains(t, names, "db.drop")

	// All emitted spans must be CLIENT kind.
	for _, s := range snaps {
		assert.Equal(t, trace.SpanKindClient, s.SpanKind(),
			"span %q: expected SpanKindClient", s.Name())
	}
}

func TestOTelDriver_DbSystemAttribute(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	drv := newTestDriver(t, tp)
	ctx := context.Background()

	require.NoError(t, drv.Lock(ctx))

	snaps := exp.GetSpans().Snapshots()
	require.Len(t, snaps, 1)
	v, ok := attrVal(snaps[0], "db.system.name")
	require.True(t, ok, "db.system.name attribute must be present on db.lock span")
	assert.Equal(t, "testdb", v)
}

func TestOTelDriver_SetVersionAttributes(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	drv := newTestDriver(t, tp)
	ctx := context.Background()

	require.NoError(t, drv.SetVersion(ctx, 42, true))

	snaps := exp.GetSpans().Snapshots()
	span, ok := findSpan(snaps, "db.set_version")
	require.True(t, ok)

	// migrate.version must be 42.
	for _, kv := range span.Attributes() {
		if string(kv.Key) == "migrate.version" {
			assert.Equal(t, int64(42), kv.Value.AsInt64())
		}
		if string(kv.Key) == "migrate.dirty" {
			assert.True(t, kv.Value.AsBool())
		}
	}
}

func TestOTelDriver_NoSpanForOpenAndClose(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	drv := newTestDriver(t, tp)
	ctx := context.Background()

	// Open and Close must not emit spans.
	_, _ = drv.Open(ctx, "stub://")
	_ = drv.Close(ctx)

	assert.Empty(t, exp.GetSpans().Snapshots(), "Open and Close must not emit spans")
}
