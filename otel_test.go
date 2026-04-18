package migrate

import (
"context"
"testing"

"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"
"go.opentelemetry.io/otel/attribute"
sdkmetric "go.opentelemetry.io/otel/sdk/metric"
"go.opentelemetry.io/otel/sdk/metric/metricdata"
sdktrace "go.opentelemetry.io/otel/sdk/trace"
"go.opentelemetry.io/otel/sdk/trace/tracetest"

sStub "github.com/golang-migrate/migrate/v4/source/stub"
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
m.sourceDrv.(*sStub.Stub).Migrations = sourceStubMigrations

// Replace global-provider instruments with test-provider instruments so
// assertions are isolated to this test run.
m.otelTracer = tp.Tracer(tracerName)
m.otelMeter = mp.Meter(meterName)
m.otelInstruments = newOtelInstruments(m.otelMeter)

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

assert.Contains(t, names, "migrate.up")
assert.Contains(t, names, "migrate.run_migration")

// sourceStubMigrations yields 5 migration attempts (versions 1, 3, 4, 5-empty, 7).
var runCount int
for _, s := range spans {
if s.Name() == "migrate.run_migration" {
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
if s.Name() == "migrate.up" {
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
assert.Contains(t, names, "migrate.down")
assert.Contains(t, names, "migrate.run_migration")
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
if s.Name() == "migrate.up" {
// codes.Error == 2; must NOT be set for ErrNoChange.
assert.NotEqual(t, sdktrace.Status{Code: 2}, s.Status(),
"ErrNoChange must not produce an error span")
}
}
}
