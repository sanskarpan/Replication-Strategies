package gateway

import (
	"bufio"
	"net"
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	status int
}

// compile-time check: responseWriter must forward Hijacker so WebSocket upgrades work.
var _ http.Hijacker = (*responseWriter)(nil)

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// Hijack forwards to the underlying ResponseWriter so WebSocket upgrades through
// the OTel middleware do not fail with "server does not support hijacking".
func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, net.ErrClosed
	}
	return hj.Hijack()
}

// otelMiddleware is a Chi middleware that starts a server-side OTel span for
// every request. The span name is "<METHOD> <route-pattern>" so Jaeger/Tempo
// group traces by endpoint rather than by URL (which would fan-out on IDs).
//
// Incoming W3C traceparent/tracestate headers are extracted so the span joins
// an existing trace started by the caller (e.g. a load test or the frontend).
func otelMiddleware(next http.Handler) http.Handler {
	tracer := otel.GetTracerProvider().Tracer("replication-strategies")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))

		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}

		ctx, span := tracer.Start(ctx, r.Method+" "+r.URL.Path,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPRequestMethodKey.String(r.Method),
				attribute.String("http.url", r.URL.String()),
				attribute.String("net.host.name", r.Host),
				attribute.String("http.scheme", scheme(r)),
			),
		)
		defer func() {
			span.SetAttributes(semconv.HTTPResponseStatusCode(rw.status))
			if rw.status >= 500 {
				span.SetStatus(codes.Error, http.StatusText(rw.status))
			}
			// Update span name to include the matched Chi route pattern.
			if pat := chi.RouteContext(r.Context()).RoutePattern(); pat != "" {
				span.SetName(r.Method + " " + pat)
			}
			span.End()
		}()

		next.ServeHTTP(rw, r.WithContext(ctx))
	})
}

func scheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	return "http"
}
