package telemetry_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"replication-strategies/internal/telemetry"
)

// newTestProvider installs an in-memory SpanRecorder as the global TracerProvider
// and returns the recorder so tests can inspect emitted spans. The caller is
// responsible for calling tp.Shutdown if needed (tests don't need to — the
// recorder never blocks).
func newTestProvider(t *testing.T) (*tracetest.SpanRecorder, func()) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return sr, func() { otel.SetTracerProvider(nil) }
}

func TestTracer_SpanCreated(t *testing.T) {
	sr, cleanup := newTestProvider(t)
	defer cleanup()

	tracer := telemetry.Tracer()
	_, span := tracer.Start(context.Background(), "test.span")
	span.End()

	spans := sr.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "test.span", spans[0].Name())
}

func TestInjectCarrier_RoundTrip(t *testing.T) {
	_, cleanup := newTestProvider(t)
	defer cleanup()

	tracer := telemetry.Tracer()
	ctx, span := tracer.Start(context.Background(), "parent")
	defer span.End()

	carrier := telemetry.InjectCarrier(ctx)
	require.NotNil(t, carrier, "active span should produce a non-nil carrier")
	assert.Contains(t, carrier, "traceparent", "carrier must hold W3C traceparent")

	restored := telemetry.ExtractCarrier(context.Background(), carrier)

	// Re-inject from the restored context and confirm the trace-id matches.
	carrier2 := telemetry.InjectCarrier(restored)
	assert.Equal(t, carrier["traceparent"], carrier2["traceparent"],
		"round-tripped context must preserve the same trace ID")
}

func TestInjectCarrier_NoSpan(t *testing.T) {
	_, cleanup := newTestProvider(t)
	defer cleanup()

	carrier := telemetry.InjectCarrier(context.Background())
	assert.Nil(t, carrier, "context without an active span should produce nil carrier")
}

func TestExtractCarrier_NilCarrier(t *testing.T) {
	ctx := telemetry.ExtractCarrier(context.Background(), nil)
	assert.NotNil(t, ctx, "ExtractCarrier with nil carrier must return a valid context")
}

func TestExtractCarrier_InvalidCarrier(t *testing.T) {
	_, cleanup := newTestProvider(t)
	defer cleanup()

	// A carrier with a garbage traceparent must not panic — OTel silently ignores it.
	bad := map[string]string{"traceparent": "not-a-valid-traceparent"}
	ctx := telemetry.ExtractCarrier(context.Background(), bad)
	assert.NotNil(t, ctx)
}
