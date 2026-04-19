package database

import (
	"context"
	"io"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/golang-migrate/migrate/v4/database"

// OTelDriver wraps a Driver and adds OpenTelemetry CLIENT spans for each
// database operation. Obtain one via NewOTelDriver and pass it to
// NewWithDatabaseInstance or NewWithInstance.
type OTelDriver struct {
	driver     Driver
	driverName string
	tracer     trace.Tracer
}

// NewOTelDriver wraps driver with OpenTelemetry instrumentation.
// driverName populates the db.system.name attribute on every span.
func NewOTelDriver(driver Driver, driverName string) Driver {
	return &OTelDriver{
		driver:     driver,
		driverName: driverName,
		tracer:     otel.GetTracerProvider().Tracer(tracerName),
	}
}

func (d *OTelDriver) attrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		semconv.DBSystemNameKey.String(d.driverName),
	}
}

func (d *OTelDriver) startSpan(ctx context.Context, name string, extra ...attribute.KeyValue) (context.Context, trace.Span) {
	attrs := append(d.attrs(), extra...)
	return d.tracer.Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)
}

func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// Unwrap returns the underlying Driver. It follows the same convention as
// errors.Unwrap and allows callers (e.g. tests) to access the inner driver.
func (d *OTelDriver) Unwrap() Driver {
	return d.driver
}

// Open delegates to the underlying driver without adding a span.
// Open is called once at construction time and is not a recurring operation.
func (d *OTelDriver) Open(ctx context.Context, url string) (Driver, error) {
	return d.driver.Open(ctx, url)
}

// Close delegates to the underlying driver without adding a span.
// Close is called once at teardown time and is not a recurring operation.
func (d *OTelDriver) Close(ctx context.Context) error {
	return d.driver.Close(ctx)
}

func (d *OTelDriver) Lock(ctx context.Context) error {
	ctx, span := d.startSpan(ctx, "db.lock")
	err := d.driver.Lock(ctx)
	endSpan(span, err)
	return err
}

func (d *OTelDriver) Unlock(ctx context.Context) error {
	ctx, span := d.startSpan(ctx, "db.unlock")
	err := d.driver.Unlock(ctx)
	endSpan(span, err)
	return err
}

func (d *OTelDriver) Run(ctx context.Context, migration io.Reader) error {
	ctx, span := d.startSpan(ctx, "db.run")
	err := d.driver.Run(ctx, migration)
	endSpan(span, err)
	return err
}

func (d *OTelDriver) SetVersion(ctx context.Context, version int, dirty bool) error {
	ctx, span := d.startSpan(ctx, "db.set_version",
		attribute.Int("migrate.version", version),
		attribute.Bool("migrate.dirty", dirty),
	)
	err := d.driver.SetVersion(ctx, version, dirty)
	endSpan(span, err)
	return err
}

func (d *OTelDriver) Version(ctx context.Context) (int, bool, error) {
	ctx, span := d.startSpan(ctx, "db.version")
	version, dirty, err := d.driver.Version(ctx)
	endSpan(span, err)
	return version, dirty, err
}

func (d *OTelDriver) Drop(ctx context.Context) error {
	ctx, span := d.startSpan(ctx, "db.drop")
	err := d.driver.Drop(ctx)
	endSpan(span, err)
	return err
}
