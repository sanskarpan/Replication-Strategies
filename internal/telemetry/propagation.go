package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
)

// mapCarrier implements propagation.TextMapCarrier over a plain map[string]string.
type mapCarrier map[string]string

func (m mapCarrier) Get(key string) string      { return m[key] }
func (m mapCarrier) Set(key, val string)         { m[key] = val }
func (m mapCarrier) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// InjectCarrier extracts the trace context from ctx and returns it as a
// map suitable for storing in events.Event.TraceCarrier.
func InjectCarrier(ctx context.Context) map[string]string {
	carrier := make(mapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	return map[string]string(carrier)
}

// ExtractCarrier restores a trace context from a carrier map (previously
// produced by InjectCarrier) and returns a context carrying that span
// reference. Returns ctx unchanged when carrier is nil.
func ExtractCarrier(ctx context.Context, carrier map[string]string) context.Context {
	if len(carrier) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, mapCarrier(carrier))
}
