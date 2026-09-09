// Package scheduler insert tasks into database
package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttam282005/distrack/internal/common"
	"github.com/uttam282005/distrack/internal/db"
	"github.com/uttam282005/distrack/internal/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type SchduleTaskRequest struct {
	Command      string `json:"command"`
	DelaySeconds int64  `json:"delay_seconds"` // ISO 8601 format
}

type TaskResponse struct {
	TaskID      string     `json:"task_id"`
	Command     string     `json:"command"`
	ScheduledAt time.Time  `json:"scheduled_at,omitempty"`
	PickedAt    *time.Time `json:"picked_at,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	FailedAt    *time.Time `json:"failed_at,omitempty"`
}

type Task struct {
	ID          string
	Command     string
	ScheduledAt time.Time
	PickedAt    *time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	FailedAt    *time.Time
}

type SchedulerServer struct {
	serverPort         string
	httpServer         *http.Server
	dbPool             *pgxpool.Pool
	dbConnectionString string
	ctx                context.Context
	cancel             context.CancelFunc
	logger             *slog.Logger
	tracer             trace.Tracer
}

func NewServer(port string, dbConnectionString string, logger *slog.Logger) *SchedulerServer {
	if logger == nil {
		logger = telemetry.InitLogger("distrack-scheduler")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &SchedulerServer{
		serverPort:         port,
		dbConnectionString: dbConnectionString,
		ctx:                ctx,
		cancel:             cancel,
		logger:             logger,
		tracer:             otel.Tracer("distrack-scheduler"),
	}
}

func (s *SchedulerServer) Start() error {
	var err error
	s.dbPool, err = db.ConnectToDatabase(s.ctx, s.dbConnectionString)
	if err != nil {
		s.logger.Error("Database connection failed", "error", err.Error())
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("/schedule", otelhttp.NewHandler(http.HandlerFunc(s.handleScheduleTask), "scheduler.schedule"))
	mux.Handle("/status", otelhttp.NewHandler(http.HandlerFunc(s.handleTaskStatus), "scheduler.status"))

	s.httpServer = &http.Server{
		Addr:    s.serverPort,
		Handler: mux,
	}

	s.logger.Info("Starting scheduler server", "port", s.serverPort)

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("Server listen and serve error", "error", err.Error())
		}
	}()

	return s.awaitShutdown()
}

func (s *SchedulerServer) handleScheduleTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST request is allowed", http.StatusMethodNotAllowed)
		return
	}

	var commandRequest SchduleTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&commandRequest); err != nil {
		s.logger.WarnContext(r.Context(), "Failed to decode schedule request", "error", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.logger.InfoContext(r.Context(), "Received schedule request",
		"command", commandRequest.Command,
		"delay_seconds", commandRequest.DelaySeconds,
	)

	if commandRequest.DelaySeconds < 0 {
		http.Error(w, "delay_seconds must be >= 0", http.StatusBadRequest)
		return
	}

	if commandRequest.DelaySeconds > 86400 { // 24h max
		http.Error(w, "delay too large", http.StatusBadRequest)
		return
	}

	scheduledTime := time.Now().UTC().Add(time.Duration(commandRequest.DelaySeconds * int64(time.Second)))

	task := Task{
		Command:     commandRequest.Command,
		ScheduledAt: scheduledTime,
	}

	taskID, err := s.insertIntoDB(r.Context(), task)
	if err != nil {
		s.logger.ErrorContext(r.Context(), "Failed to submit task", "error", err.Error())
		http.Error(w, fmt.Sprintf("Failed to submit task. Error: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	commandResponse := TaskResponse{
		TaskID:      taskID,
		Command:     commandRequest.Command,
		ScheduledAt: scheduledTime,
		FailedAt:    nil,
		CompletedAt: nil,
		StartedAt:   nil,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(commandResponse); err != nil {
		s.logger.ErrorContext(r.Context(), "Failed to encode response", "error", err.Error())
	}
}

func (s *SchedulerServer) handleTaskStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}

	taskID := r.URL.Query().Get("task_id")
	if taskID == "" {
		http.Error(w, "Task ID is required", http.StatusBadRequest)
		return
	}

	ctx, span := s.tracer.Start(r.Context(), "db.get_task_status",
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("task.id", taskID),
		),
	)
	defer span.End()

	var task Task
	err := s.dbPool.QueryRow(
		ctx,
		`SELECT id, command, scheduled_at, picked_at, started_at, completed_at, failed_at
         FROM tasks WHERE id = $1`,
		taskID,
	).Scan(
		&task.ID,
		&task.Command,
		&task.ScheduledAt,
		&task.PickedAt,
		&task.StartedAt,
		&task.CompletedAt,
		&task.FailedAt,
	)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		s.logger.WarnContext(ctx, "Task not found", "task_id", taskID, "error", err.Error())
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	response := TaskResponse{
		TaskID:      task.ID,
		Command:     task.Command,
		ScheduledAt: task.ScheduledAt.UTC(),
		PickedAt:    common.ToUTC(task.PickedAt),
		StartedAt:   common.ToUTC(task.StartedAt),
		CompletedAt: common.ToUTC(task.CompletedAt),
		FailedAt:    common.ToUTC(task.FailedAt),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.ErrorContext(ctx, "Failed to encode response", "error", err.Error())
	}
}

func (s *SchedulerServer) insertIntoDB(ctx context.Context, task Task) (string, error) {
	ctx, span := s.tracer.Start(ctx, "db.insert_task",
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("task.command", task.Command),
		),
	)
	defer span.End()

	traceparent := telemetry.InjectTraceparent(ctx)
	sqlStatement := "INSERT INTO tasks (command, scheduled_at, traceparent) VALUES ($1, $2, $3) RETURNING id"

	var taskID string
	err := s.dbPool.QueryRow(ctx, sqlStatement, task.Command, task.ScheduledAt, traceparent).Scan(&taskID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	span.SetAttributes(attribute.String("task.id", taskID))
	return taskID, nil
}

func (s *SchedulerServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	return s.Stop()
}

func (s *SchedulerServer) Stop() error {
	if s.dbPool != nil {
		s.dbPool.Close()
	}
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(ctx)
	}

	s.logger.Info("Scheduler server and database pool stopped")
	return nil
}
