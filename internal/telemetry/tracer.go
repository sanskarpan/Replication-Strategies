// Package telemetry wires up OpenTelemetry distributed tracing for the
// replication-strategies simulator.
//
// Set OTEL_ENABLED=true (and optionally OTEL_EXPORTER_OTLP_ENDPOINT) to
// activate live trace export; otherwise the provider is a no-op and the binary
// incurs no overhead.
package telemetry

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const instrumentationScope = "replication-strategies"

// Init configures the global TracerProvider and TextMapPropagator.
//
// When OTEL_ENABLED=true it installs an OTLP/HTTP exporter aimed at
// OTEL_EXPORTER_OTLP_ENDPOINT (default http://localhost:4318).
// When OTEL_TRACES_STDOUT=true it additionally prints spans to stdout.
// Otherwise a no-op provider is installed so all trace calls are zero-cost.
//
// The returned shutdown function must be deferred by the caller to flush spans.
func Init(ctx context.Context, serviceName, serviceVersion string) (shutdown func(context.Context) error, err error) {
	if os.Getenv("OTEL_ENABLED") != "true" {
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
		),
	)
	if err != nil {
		return nil, err
	}

	var exporters []sdktrace.SpanExporter

	otlpExp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	exporters = append(exporters, otlpExp)

	if os.Getenv("OTEL_TRACES_STDOUT") == "true" {
		stdExp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, err
		}
		exporters = append(exporters, stdExp)
	}

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	}
	for _, exp := range exporters {
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	tp := sdktrace.NewTracerProvider(opts...)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// Tracer returns a named tracer from the global provider.
func Tracer() trace.Tracer {
	return otel.GetTracerProvider().Tracer(instrumentationScope)
}
