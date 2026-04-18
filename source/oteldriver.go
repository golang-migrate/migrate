package source

import (
	"context"
	"errors"
	"io"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/golang-migrate/migrate/v4/source"

// OTelDriver wraps a Driver and adds OpenTelemetry INTERNAL spans for ReadUp
// and ReadDown. Obtain one via NewOTelDriver and pass it to
// NewWithSourceInstance or NewWithInstance.
type OTelDriver struct {
	driver     Driver
	sourceName string
	tracer     trace.Tracer
}

// NewOTelDriver wraps driver with OpenTelemetry instrumentation.
// sourceName populates the migrate.source attribute on every span.
func NewOTelDriver(driver Driver, sourceName string) Driver {
	return &OTelDriver{
		driver:     driver,
		sourceName: sourceName,
		tracer:     otel.GetTracerProvider().Tracer(tracerName),
	}
}

func (d *OTelDriver) attrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("migrate.source", d.sourceName),
	}
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

// First delegates without a span — in-memory map lookup in every driver.
func (d *OTelDriver) First(ctx context.Context) (uint, error) {
	return d.driver.First(ctx)
}

// Prev delegates without a span — in-memory map lookup in every driver.
func (d *OTelDriver) Prev(ctx context.Context, version uint) (uint, error) {
	return d.driver.Prev(ctx, version)
}

// Next delegates without a span — in-memory map lookup in every driver.
func (d *OTelDriver) Next(ctx context.Context, version uint) (uint, error) {
	return d.driver.Next(ctx, version)
}

func (d *OTelDriver) ReadUp(ctx context.Context, version uint) (io.ReadCloser, string, error) {
	ctx, span := d.tracer.Start(ctx, "source.read_up",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(append(d.attrs(), attribute.Int("migrate.version", int(version)))...),
	)
	r, identifier, err := d.driver.ReadUp(ctx, version)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
	return r, identifier, err
}

func (d *OTelDriver) ReadDown(ctx context.Context, version uint) (io.ReadCloser, string, error) {
	ctx, span := d.tracer.Start(ctx, "source.read_down",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(append(d.attrs(), attribute.Int("migrate.version", int(version)))...),
	)
	r, identifier, err := d.driver.ReadDown(ctx, version)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
	return r, identifier, err
}
