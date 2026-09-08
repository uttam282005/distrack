package telemetry

import (
	"context"
	"io"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// traceContextHandler is a slog.Handler middleware that automatically extracts
// the active OpenTelemetry trace_id and span_id from context and attaches them as JSON fields.
type traceContextHandler struct {
	slog.Handler
}

// Handle inspects the context for an active span and enriches the log record.
func (h *traceContextHandler) Handle(ctx context.Context, record slog.Record) error {
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		record.AddAttrs(
			slog.String("trace_id", span.SpanContext().TraceID().String()),
			slog.String("span_id", span.SpanContext().SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, record)
}

// WithAttrs returns a new handler whose attributes include the given attributes.
func (h *traceContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceContextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

// WithGroup returns a new handler that starts any generated attributes with the given group name.
func (h *traceContextHandler) WithGroup(name string) slog.Handler {
	return &traceContextHandler{Handler: h.Handler.WithGroup(name)}
}

// InitLogger creates a structured JSON logger writing to stdout with service name metadata
// and automatic trace-context enrichment.
func InitLogger(serviceName string) *slog.Logger {
	return NewLoggerWithWriter(serviceName, os.Stdout, slog.LevelInfo)
}

// NewLoggerWithWriter creates a logger with a custom io.Writer and level, primarily for testing.
func NewLoggerWithWriter(serviceName string, w io.Writer, level slog.Level) *slog.Logger {
	base := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
	})
	handler := &traceContextHandler{Handler: base}
	return slog.New(handler).With(slog.String("service", serviceName))
}
