package scheduler

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/uttam282005/distrack/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func init() {
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
}

func TestScheduleHandlerValidation(t *testing.T) {
	logger := telemetry.NewLoggerWithWriter("test-scheduler", io.Discard, slog.LevelInfo)
	server := NewServer(":8081", "", logger)

	tests := []struct {
		name           string
		method         string
		body           string
		expectedStatus int
	}{
		{
			name:           "MethodNotAllowed for GET",
			method:         http.MethodGet,
			body:           "",
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "BadRequest for invalid JSON",
			method:         http.MethodPost,
			body:           "{invalid-json}",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "BadRequest for negative delay",
			method:         http.MethodPost,
			body:           `{"command":"echo test", "delay_seconds": -5}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "BadRequest for excessive delay",
			method:         http.MethodPost,
			body:           `{"command":"echo test", "delay_seconds": 999999}`,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "/schedule", bytes.NewBufferString(tc.body))
			rec := httptest.NewRecorder()

			server.handleScheduleTask(rec, req)

			if rec.Code != tc.expectedStatus {
				t.Errorf("expected status %d, got %d", tc.expectedStatus, rec.Code)
			}
		})
	}
}

func TestStatusHandlerValidation(t *testing.T) {
	logger := telemetry.NewLoggerWithWriter("test-scheduler", io.Discard, slog.LevelInfo)
	server := NewServer(":8081", "", logger)

	// GET without task_id
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rec := httptest.NewRecorder()
	server.handleTaskStatus(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status %d for missing task_id, got %d", http.StatusBadRequest, rec.Code)
	}

	// POST to status endpoint should be MethodNotAllowed
	reqPost := httptest.NewRequest(http.MethodPost, "/status?task_id=123", nil)
	recPost := httptest.NewRecorder()
	server.handleTaskStatus(recPost, reqPost)
	if recPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d", http.StatusMethodNotAllowed, recPost.Code)
	}
}

func TestSchedulerTraceparentGeneration(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer tp.Shutdown(context.Background())
	tracer := tp.Tracer("test")

	ctx, span := tracer.Start(context.Background(), "test-parent")
	defer span.End()

	traceparent := telemetry.InjectTraceparent(ctx)
	if traceparent == "" {
		t.Fatal("expected traceparent to be generated from active context")
	}

	extracted := telemetry.ExtractTraceparent(context.Background(), traceparent)
	if extracted == nil {
		t.Fatal("expected non-nil extracted context")
	}
}
