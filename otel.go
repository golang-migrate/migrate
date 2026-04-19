package migrate

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	// instrumentationName is the instrumentation scope name used when creating
	// the tracer and meter.
	instrumentationName = "github.com/golang-migrate/migrate/v4"
)

// otelInstruments holds pre-created OTel metric instruments for a Migrate instance.
type otelInstruments struct {
	// migrationsApplied counts successfully applied migrations.
	migrationsApplied metric.Int64Counter

	// migrationsFailed counts failed migration applications.
	migrationsFailed metric.Int64Counter

	// migrationRunDuration records the execution duration of databaseDrv.Run per migration.
	migrationRunDuration metric.Float64Histogram
}

// newOtelInstruments creates metric instruments from the provided meter.
// On any instrument creation error, the OTel global error handler is invoked
// and the corresponding instrument is replaced with a no-op (the OTel API
// guarantees no-op instruments are returned on error, so callers are safe).
func newOtelInstruments(meter metric.Meter) otelInstruments {
	applied, _ := meter.Int64Counter(
		"migrate.migrations.applied",
		metric.WithDescription("Number of migrations successfully applied."),
		metric.WithUnit("{migration}"),
	)

	failed, _ := meter.Int64Counter(
		"migrate.migrations.failed",
		metric.WithDescription("Number of migrations that failed to apply."),
		metric.WithUnit("{migration}"),
	)

	duration, _ := meter.Float64Histogram(
		"migrate.migration.run.duration",
		metric.WithDescription("Execution duration of a single migration run against the database."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10),
	)

	return otelInstruments{
		migrationsApplied:    applied,
		migrationsFailed:     failed,
		migrationRunDuration: duration,
	}
}

// newTracer returns a tracer from the global TracerProvider.
func newTracer() trace.Tracer {
	return otel.GetTracerProvider().Tracer(instrumentationName)
}

// newMeter returns a meter from the global MeterProvider.
func newMeter() metric.Meter {
	return otel.GetMeterProvider().Meter(instrumentationName)
}
