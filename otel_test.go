package migrate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const (
	spanNameUp           = "migrate.up"
	spanNameDown         = "migrate.down"
	spanNameRunMigration = "migrate.run_migration"
)

// setupOtelTest creates a *Migrate instance wired to in-process OTel SDK providers
// (in-memory span exporter + manual metric reader). It reuses sourceStubMigrations
// defined in migrate_test.go (versions 1, 3, 4, 5, 7 — 4 up-migrations). The
// returned cleanup func shuts down both providers.
func setupOtelTest(t *testing.T) (
	m *Migrate,
	spanExporter *tracetest.InMemoryExporter,
	metricReader sdkmetric.Reader,
	cleanup func(),
) {
	t.Helper()

	spanExporter = tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spanExporter))

	metricReader = sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))

	var err error
	m, err = New(context.Background(), "stub://", "stub://")
	require.NoError(t, err)

	// Use the shared stub migrations (versions 1, 3, 4, 5, 7).
	sourceStub(m).Migrations = sourceStubMigrations

	// Replace global-provider instruments with test-provider instruments so
	// assertions are isolated to this test run.
	m.otelTracer = tp.Tracer(instrumentationName)
	meter := mp.Meter(instrumentationName)
	m.otelInstruments = newOtelInstruments(meter)

	cleanup = func() {
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
	}
	return m, spanExporter, metricReader, cleanup
}

// spanNames returns the operation names of all recorded spans.
func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	names := make([]string, len(spans))
	for i, s := range spans {
		names[i] = s.Name()
	}
	return names
}

// findCounter returns the summed value and presence of a named Int64 counter.
func findCounter(rm metricdata.ResourceMetrics, name string) (int64, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				data, ok := m.Data.(metricdata.Sum[int64])
				if !ok {
					return 0, false
				}
				var total int64
				for _, dp := range data.DataPoints {
					total += dp.Value
				}
				return total, true
			}
		}
	}
	return 0, false
}

// hasHistogramData returns true if the named histogram has at least one data point.
func hasHistogramData(rm metricdata.ResourceMetrics, name string) bool {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				data, ok := m.Data.(metricdata.Histogram[float64])
				return ok && len(data.DataPoints) > 0
			}
		}
	}
	return false
}

// attrVal finds an attribute value by key in a span's attribute set.
func attrVal(span sdktrace.ReadOnlySpan, key string) (attribute.Value, bool) {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

// TestOtelUp_Spans verifies that Up() emits a parent "migrate.up" span and one
// "migrate.run_migration" child span per migration.
func TestOtelUp_Spans(t *testing.T) {
	m, spanExporter, _, cleanup := setupOtelTest(t)
	defer cleanup()

	require.NoError(t, m.Up(context.Background()))

	spans := spanExporter.GetSpans().Snapshots()
	names := spanNames(spans)

	assert.Contains(t, names, spanNameUp)
	assert.Contains(t, names, spanNameRunMigration)

	// sourceStubMigrations yields 5 migration attempts (versions 1, 3, 4, 5-empty, 7).
	var runCount int
	for _, s := range spans {
		if s.Name() == spanNameRunMigration {
			runCount++
		}
	}
	assert.Equal(t, 5, runCount)
}

// TestOtelUp_ParentSpanAttributes verifies required attributes on the "migrate.up" span.
func TestOtelUp_ParentSpanAttributes(t *testing.T) {
	m, spanExporter, _, cleanup := setupOtelTest(t)
	defer cleanup()

	require.NoError(t, m.Up(context.Background()))

	spans := spanExporter.GetSpans().Snapshots()
	var parentSpan sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.Name() == spanNameUp {
			parentSpan = s
			break
		}
	}
	require.NotNil(t, parentSpan)

	dir, ok := attrVal(parentSpan, "migrate.direction")
	require.True(t, ok, "migrate.direction must be present")
	assert.Equal(t, "up", dir.AsString())
}

// TestOtelUp_Metrics verifies applied counter and duration histogram on success.
func TestOtelUp_Metrics(t *testing.T) {
	m, _, metricReader, cleanup := setupOtelTest(t)
	defer cleanup()

	require.NoError(t, m.Up(context.Background()))

	var rm metricdata.ResourceMetrics
	require.NoError(t, metricReader.Collect(context.Background(), &rm))

	applied, ok := findCounter(rm, "migrate.migrations.applied")
	assert.True(t, ok, "migrate.migrations.applied counter must exist")
	// sourceStubMigrations yields 5 migration attempts (version 5 is empty-body up).
	assert.Equal(t, int64(5), applied)

	failed, ok := findCounter(rm, "migrate.migrations.failed")
	if ok {
		assert.Equal(t, int64(0), failed)
	}

	assert.True(t, hasHistogramData(rm, "migrate.migration.run.duration"))
}

// TestOtelDown_Spans verifies Down() emits "migrate.down" and per-migration spans.
func TestOtelDown_Spans(t *testing.T) {
	m, spanExporter, _, cleanup := setupOtelTest(t)
	defer cleanup()

	require.NoError(t, m.Up(context.Background()))
	spanExporter.Reset()

	require.NoError(t, m.Down(context.Background()))

	names := spanNames(spanExporter.GetSpans().Snapshots())
	assert.Contains(t, names, spanNameDown)
	assert.Contains(t, names, spanNameRunMigration)
}

// TestOtelMigrate_Spans verifies Migrate() emits "migrate.migrate" with a version attribute.
func TestOtelMigrate_Spans(t *testing.T) {
	m, spanExporter, _, cleanup := setupOtelTest(t)
	defer cleanup()

	require.NoError(t, m.Migrate(context.Background(), 3))

	spans := spanExporter.GetSpans().Snapshots()
	var parentSpan sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.Name() == "migrate.migrate" {
			parentSpan = s
			break
		}
	}
	require.NotNil(t, parentSpan, "migrate.migrate span must be emitted")

	v, ok := attrVal(parentSpan, "migrate.version")
	require.True(t, ok, "migrate.version attribute must be set")
	assert.Equal(t, int64(3), v.AsInt64())
}

// TestOtelSteps_Spans verifies Steps() emits "migrate.steps" with a direction attribute.
func TestOtelSteps_Spans(t *testing.T) {
	m, spanExporter, _, cleanup := setupOtelTest(t)
	defer cleanup()

	require.NoError(t, m.Steps(context.Background(), 1))

	spans := spanExporter.GetSpans().Snapshots()
	var parentSpan sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.Name() == "migrate.steps" {
			parentSpan = s
			break
		}
	}
	require.NotNil(t, parentSpan, "migrate.steps span must be emitted")

	dir, ok := attrVal(parentSpan, "migrate.direction")
	require.True(t, ok)
	assert.Equal(t, "up", dir.AsString())
}

// TestOtelForce_Spans verifies Force() emits "migrate.force" with a version attribute.
func TestOtelForce_Spans(t *testing.T) {
	m, spanExporter, _, cleanup := setupOtelTest(t)
	defer cleanup()

	require.NoError(t, m.Force(context.Background(), 1))

	spans := spanExporter.GetSpans().Snapshots()
	assert.Contains(t, spanNames(spans), "migrate.force")

	for _, s := range spans {
		if s.Name() == "migrate.force" {
			v, ok := attrVal(s, "migrate.version")
			require.True(t, ok)
			assert.Equal(t, int64(1), v.AsInt64())
		}
	}
}

// TestOtelDrop_Spans verifies Drop() emits a "migrate.drop" span.
func TestOtelDrop_Spans(t *testing.T) {
	m, spanExporter, _, cleanup := setupOtelTest(t)
	defer cleanup()

	require.NoError(t, m.Up(context.Background()))
	spanExporter.Reset()

	require.NoError(t, m.Drop(context.Background()))

	assert.Contains(t, spanNames(spanExporter.GetSpans().Snapshots()), "migrate.drop")
}

// TestOtelNoChange_NoErrorSpan verifies ErrNoChange does not set span status to Error.
func TestOtelNoChange_NoErrorSpan(t *testing.T) {
	m, spanExporter, _, cleanup := setupOtelTest(t)
	defer cleanup()

	require.NoError(t, m.Up(context.Background()))
	spanExporter.Reset()

	err := m.Up(context.Background())
	assert.Equal(t, ErrNoChange, err)

	for _, s := range spanExporter.GetSpans().Snapshots() {
		if s.Name() == spanNameUp {
			// codes.Error == 2; must NOT be set for ErrNoChange.
			assert.NotEqual(t, sdktrace.Status{Code: 2}, s.Status(),
				"ErrNoChange must not produce an error span")
		}
	}
}

// setupOtelTopologyTest sets the global OTel provider to an in-memory exporter,
// then creates a Migrate instance so that both the core tracer and the
// database/source OTelDriver wrappers all emit to the same exporter.
func setupOtelTopologyTest(t *testing.T) (m *Migrate, exp *tracetest.InMemoryExporter) {
	t.Helper()

	exp = tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
	})

	var err error
	m, err = New(context.Background(), "stub://", "stub://")
	require.NoError(t, err)

	sourceStub(m).Migrations = sourceStubMigrations

	// Replace the core tracer so it uses the same test provider.
	m.otelTracer = tp.Tracer(instrumentationName)
	return m, exp
}

// TestOtelTraceTopology verifies that db.set_version and db.run spans are
// children of the migrate.run_migration span, and that migrate.run_migration
// is a child of migrate.up.
func TestOtelTraceTopology(t *testing.T) {
	m, exp := setupOtelTopologyTest(t)

	require.NoError(t, m.Up(context.Background()))

	snaps := exp.GetSpans().Snapshots()

	// Index spans by spanID for parent lookup.
	type spanInfo struct {
		name     string
		spanID   trace.SpanID
		parentID trace.SpanID
	}
	var byID = make(map[trace.SpanID]spanInfo)
	for _, s := range snaps {
		byID[s.SpanContext().SpanID()] = spanInfo{
			name:     s.Name(),
			spanID:   s.SpanContext().SpanID(),
			parentID: s.Parent().SpanID(),
		}
	}

	// Find migrate.up span.
	var upSpanID trace.SpanID
	for _, info := range byID {
		if info.name == spanNameUp {
			upSpanID = info.spanID
			break
		}
	}
	require.NotEqual(t, trace.SpanID{}, upSpanID, "migrate.up span must exist")

	// Every migrate.run_migration must be a direct child of migrate.up.
	// Collect children of each migrate.run_migration to verify db.* nesting.
	var foundRunMigr int
	var foundRunMigrWithDBRun int
	for _, info := range byID {
		if info.name != spanNameRunMigration {
			continue
		}
		foundRunMigr++
		assert.Equal(t, upSpanID, info.parentID,
			"migrate.run_migration must be a child of migrate.up")

		runMigrSpanID := info.spanID
		var childNames []string
		for _, child := range byID {
			if child.parentID == runMigrSpanID {
				childNames = append(childNames, child.name)
			}
		}

		// All run_migration spans emit at least two db.set_version calls
		// (dirty=true before, dirty=false after). db.run is only present
		// when the migration has a body (version 5 has no Up body).
		assert.Contains(t, childNames, "db.set_version",
			"db.set_version must be a child of migrate.run_migration")

		// Track how many have db.run to assert at least one does.
		for _, n := range childNames {
			if n == "db.run" {
				foundRunMigrWithDBRun++
				break
			}
		}
	}
	require.Greater(t, foundRunMigr, 0, "at least one migrate.run_migration span expected")
	assert.Greater(t, foundRunMigrWithDBRun, 0,
		"at least one migrate.run_migration must have db.run as a child")
}
