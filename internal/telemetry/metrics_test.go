package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMetricsInitializationAndRecording(t *testing.T) {
	// Initialize a test MeterProvider with a manual reader (no network exporter needed for unit tests)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())
	otel.SetMeterProvider(mp)

	ctx := context.Background()

	// 1. Test SchedulerMetrics
	sm, err := NewSchedulerMetrics()
	if err != nil {
		t.Fatalf("failed to initialize SchedulerMetrics: %v", err)
	}
	sm.RecordHTTPRequest(ctx, "schedule", "POST", 200, 0.042)
	sm.RecordTaskScheduled(ctx, "success")

	// 2. Test CoordinatorMetrics
	cm, err := NewCoordinatorMetrics()
	if err != nil {
		t.Fatalf("failed to initialize CoordinatorMetrics: %v", err)
	}
	cm.RecordTaskDispatched(ctx, "success", 0.015)
	cm.RecordHeartbeat(ctx, "success")
	cm.RecordHeartbeatFailure(ctx)
	cm.AddActiveWorkers(ctx, 1)
	cm.AddActiveWorkers(ctx, -1)
	cm.RecordStatusUpdate(ctx, "COMPLETED")

	// 3. Test WorkerMetrics
	wm, err := NewWorkerMetrics()
	if err != nil {
		t.Fatalf("failed to initialize WorkerMetrics: %v", err)
	}
	wm.RecordTaskCompletion(ctx, "command", "success", 0.125)
	wm.AddActiveTasks(ctx, 1)
	wm.AddActiveTasks(ctx, -1)
	wm.AddQueueDepth(ctx, 1)
	wm.AddQueueDepth(ctx, -1)
	wm.RecordHeartbeatSent(ctx, "success")

	// Collect metrics from reader and verify data exists
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("failed to collect test metrics: %v", err)
	}

	if len(rm.ScopeMetrics) == 0 {
		t.Fatal("expected at least one ScopeMetrics, got none")
	}
}
