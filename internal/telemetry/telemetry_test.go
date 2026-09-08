package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceparentInjectionAndExtraction(t *testing.T) {
	// Setup tracer provider and propagator
	tp := sdktrace.NewTracerProvider()
	defer tp.Shutdown(context.Background())
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tracer := tp.Tracer("test-tracer")
	ctx, span := tracer.Start(context.Background(), "test-span")
	defer span.End()

	spanCtx := span.SpanContext()
	if !spanCtx.IsValid() {
		t.Fatal("expected valid span context")
	}

	// 1. Inject into traceparent string
	tpStr := InjectTraceparent(ctx)
	if tpStr == "" {
		t.Fatal("expected non-empty traceparent string")
	}

	// W3C traceparent must start with 00- and contain the 32-hex trace ID
	expectedTraceID := spanCtx.TraceID().String()
	if !strings.Contains(tpStr, expectedTraceID) {
		t.Fatalf("expected traceparent %q to contain trace ID %q", tpStr, expectedTraceID)
	}

	// 2. Extract into new context
	extractedCtx := ExtractTraceparent(context.Background(), tpStr)
	extractedSpan := trace.SpanFromContext(extractedCtx)
	extractedSpanCtx := extractedSpan.SpanContext()

	if !extractedSpanCtx.IsValid() {
		t.Fatal("expected extracted span context to be valid")
	}

	if extractedSpanCtx.TraceID().String() != expectedTraceID {
		t.Fatalf("trace ID mismatch: got %s, want %s", extractedSpanCtx.TraceID().String(), expectedTraceID)
	}
}

func TestLoggerTraceCorrelation(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer tp.Shutdown(context.Background())
	otel.SetTracerProvider(tp)

	var buf bytes.Buffer
	logger := NewLoggerWithWriter("test-service", &buf, 0)

	tracer := tp.Tracer("test-logger")
	ctx, span := tracer.Start(context.Background(), "logged-operation")
	defer span.End()

	expectedTraceID := span.SpanContext().TraceID().String()
	expectedSpanID := span.SpanContext().SpanID().String()

	logger.InfoContext(ctx, "hello structured log", "task_id", "task-123")

	var logEntry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("failed to unmarshal log entry: %v\nOutput was: %s", err, buf.String())
	}

	if logEntry["service"] != "test-service" {
		t.Errorf("expected service 'test-service', got %v", logEntry["service"])
	}
	if logEntry["trace_id"] != expectedTraceID {
		t.Errorf("expected trace_id %s, got %v", expectedTraceID, logEntry["trace_id"])
	}
	if logEntry["span_id"] != expectedSpanID {
		t.Errorf("expected span_id %s, got %v", expectedSpanID, logEntry["span_id"])
	}
	if logEntry["task_id"] != "task-123" {
		t.Errorf("expected task_id 'task-123', got %v", logEntry["task_id"])
	}
}
