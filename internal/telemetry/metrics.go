// Package telemetry provides metrics instrumentation helpers for distrack services.
package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// SchedulerMetrics tracks RED and business metrics for the Scheduler service.
type SchedulerMetrics struct {
	httpRequestsTotal   metric.Int64Counter
	httpRequestDuration metric.Float64Histogram
	tasksScheduledTotal metric.Int64Counter
}

// NewSchedulerMetrics initializes and registers scheduler metrics with the global MeterProvider.
func NewSchedulerMetrics() (*SchedulerMetrics, error) {
	meter := otel.GetMeterProvider().Meter("distrack.scheduler")

	httpReqs, err := meter.Int64Counter(
		"http_requests_total",
		metric.WithDescription("Total HTTP requests handled by the scheduler"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create http_requests_total: %w", err)
	}

	httpDuration, err := meter.Float64Histogram(
		"http_request_duration_seconds",
		metric.WithDescription("HTTP request latency distribution in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create http_request_duration_seconds: %w", err)
	}

	tasksScheduled, err := meter.Int64Counter(
		"tasks_scheduled_total",
		metric.WithDescription("Total number of tasks scheduled"),
		metric.WithUnit("{task}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create tasks_scheduled_total: %w", err)
	}

	return &SchedulerMetrics{
		httpRequestsTotal:   httpReqs,
		httpRequestDuration: httpDuration,
		tasksScheduledTotal: tasksScheduled,
	}, nil
}

// RecordHTTPRequest records an inbound HTTP request count and its processing duration.
func (m *SchedulerMetrics) RecordHTTPRequest(ctx context.Context, handler, method string, statusCode int, durationSec float64) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("handler", handler),
		attribute.String("method", method),
		attribute.Int("status", statusCode),
	)
	m.httpRequestsTotal.Add(ctx, 1, attrs)
	m.httpRequestDuration.Record(ctx, durationSec, attrs)
}

// RecordTaskScheduled records a scheduled task event with its status (e.g. success, error).
func (m *SchedulerMetrics) RecordTaskScheduled(ctx context.Context, status string) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("status", status))
	m.tasksScheduledTotal.Add(ctx, 1, attrs)
}

// CoordinatorMetrics tracks RED and USE metrics for the Coordinator service.
type CoordinatorMetrics struct {
	tasksDispatchedTotal   metric.Int64Counter
	dispatchDuration       metric.Float64Histogram
	activeWorkers          metric.Int64UpDownCounter
	heartbeatsTotal        metric.Int64Counter
	heartbeatFailuresTotal metric.Int64Counter
	taskStatusUpdatesTotal metric.Int64Counter
}

// NewCoordinatorMetrics initializes and registers coordinator metrics with the global MeterProvider.
func NewCoordinatorMetrics() (*CoordinatorMetrics, error) {
	meter := otel.GetMeterProvider().Meter("distrack.coordinator")

	dispatched, err := meter.Int64Counter(
		"coordinator_tasks_dispatched_total",
		metric.WithDescription("Total number of tasks dispatched to workers"),
		metric.WithUnit("{task}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create coordinator_tasks_dispatched_total: %w", err)
	}

	duration, err := meter.Float64Histogram(
		"coordinator_dispatch_duration_seconds",
		metric.WithDescription("Latency taken to dispatch task to worker in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create coordinator_dispatch_duration_seconds: %w", err)
	}

	active, err := meter.Int64UpDownCounter(
		"coordinator_active_workers",
		metric.WithDescription("Current number of active registered workers in coordinator pool"),
		metric.WithUnit("{worker}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create coordinator_active_workers: %w", err)
	}

	hb, err := meter.Int64Counter(
		"coordinator_worker_heartbeats_total",
		metric.WithDescription("Total number of heartbeats received from workers"),
		metric.WithUnit("{heartbeat}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create coordinator_worker_heartbeats_total: %w", err)
	}

	hbFailures, err := meter.Int64Counter(
		"coordinator_worker_heartbeat_failures_total",
		metric.WithDescription("Total number of missed heartbeats or worker dropouts"),
		metric.WithUnit("{failure}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create coordinator_worker_heartbeat_failures_total: %w", err)
	}

	updates, err := meter.Int64Counter(
		"coordinator_task_status_updates_total",
		metric.WithDescription("Total task status updates received from workers"),
		metric.WithUnit("{update}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create coordinator_task_status_updates_total: %w", err)
	}

	return &CoordinatorMetrics{
		tasksDispatchedTotal:   dispatched,
		dispatchDuration:       duration,
		activeWorkers:          active,
		heartbeatsTotal:        hb,
		heartbeatFailuresTotal: hbFailures,
		taskStatusUpdatesTotal: updates,
	}, nil
}

// RecordTaskDispatched records a dispatched task and the time spent dispatching it.
func (m *CoordinatorMetrics) RecordTaskDispatched(ctx context.Context, status string, durationSec float64) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("status", status))
	m.tasksDispatchedTotal.Add(ctx, 1, attrs)
	m.dispatchDuration.Record(ctx, durationSec, attrs)
}

// RecordHeartbeat records an incoming heartbeat event.
func (m *CoordinatorMetrics) RecordHeartbeat(ctx context.Context, status string) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("status", status))
	m.heartbeatsTotal.Add(ctx, 1, attrs)
}

// RecordHeartbeatFailure records a missed worker heartbeat.
func (m *CoordinatorMetrics) RecordHeartbeatFailure(ctx context.Context) {
	if m == nil {
		return
	}
	m.heartbeatFailuresTotal.Add(ctx, 1)
}

// AddActiveWorkers adjusts the current count of active workers in the pool.
func (m *CoordinatorMetrics) AddActiveWorkers(ctx context.Context, delta int64) {
	if m == nil {
		return
	}
	m.activeWorkers.Add(ctx, delta)
}

// RecordStatusUpdate records a task completion or failure update reported by a worker.
func (m *CoordinatorMetrics) RecordStatusUpdate(ctx context.Context, status string) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("status", status))
	m.taskStatusUpdatesTotal.Add(ctx, 1, attrs)
}

// WorkerMetrics tracks RED and USE metrics for the Worker service.
type WorkerMetrics struct {
	tasksExecutedTotal    metric.Int64Counter
	taskExecutionDuration metric.Float64Histogram
	activeTasks           metric.Int64UpDownCounter
	queueDepth            metric.Int64UpDownCounter
	heartbeatsSentTotal   metric.Int64Counter
}

// NewWorkerMetrics initializes and registers worker metrics with the global MeterProvider.
func NewWorkerMetrics() (*WorkerMetrics, error) {
	meter := otel.GetMeterProvider().Meter("distrack.worker")

	executed, err := meter.Int64Counter(
		"tasks_executed_total",
		metric.WithDescription("Total number of tasks executed by worker nodes"),
		metric.WithUnit("{task}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create tasks_executed_total: %w", err)
	}

	duration, err := meter.Float64Histogram(
		"task_execution_duration_seconds",
		metric.WithDescription("Duration of task execution in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create task_execution_duration_seconds: %w", err)
	}

	active, err := meter.Int64UpDownCounter(
		"worker_active_tasks",
		metric.WithDescription("Currently executing concurrent tasks"),
		metric.WithUnit("{task}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create worker_active_tasks: %w", err)
	}

	queue, err := meter.Int64UpDownCounter(
		"worker_queue_depth",
		metric.WithDescription("Current number of tasks buffered in worker queue"),
		metric.WithUnit("{task}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create worker_queue_depth: %w", err)
	}

	hb, err := meter.Int64Counter(
		"worker_heartbeats_total",
		metric.WithDescription("Total number of heartbeats sent to coordinator"),
		metric.WithUnit("{heartbeat}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create worker_heartbeats_total: %w", err)
	}

	return &WorkerMetrics{
		tasksExecutedTotal:    executed,
		taskExecutionDuration: duration,
		activeTasks:           active,
		queueDepth:            queue,
		heartbeatsSentTotal:   hb,
	}, nil
}

// RecordTaskCompletion records a finished task execution with its duration and status.
func (m *WorkerMetrics) RecordTaskCompletion(ctx context.Context, taskType string, status string, durationSec float64) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("task_type", taskType),
		attribute.String("status", status),
	)
	m.tasksExecutedTotal.Add(ctx, 1, attrs)
	m.taskExecutionDuration.Record(ctx, durationSec, attrs)
}

// AddActiveTasks adjusts the count of currently running tasks on this worker.
func (m *WorkerMetrics) AddActiveTasks(ctx context.Context, delta int64) {
	if m == nil {
		return
	}
	m.activeTasks.Add(ctx, delta)
}

// AddQueueDepth adjusts the count of tasks currently queued in the channel.
func (m *WorkerMetrics) AddQueueDepth(ctx context.Context, delta int64) {
	if m == nil {
		return
	}
	m.queueDepth.Add(ctx, delta)
}

// RecordHeartbeatSent records an outbound heartbeat to coordinator.
func (m *WorkerMetrics) RecordHeartbeatSent(ctx context.Context, status string) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("status", status))
	m.heartbeatsSentTotal.Add(ctx, 1, attrs)
}
