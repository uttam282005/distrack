// Package scheduler insert tasks into database
package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttam282005/distrack/internal/common"
	"github.com/uttam282005/distrack/internal/db"
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
}

func NewServer(port string, dbConnectionString string) *SchedulerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &SchedulerServer{
		serverPort:         port,
		dbConnectionString: dbConnectionString,
		ctx:                ctx,
		cancel:             cancel,
	}
}

func (s *SchedulerServer) Start() error {
	var err error
	s.dbPool, err = db.ConnectToDatabase(s.ctx, s.dbConnectionString)
	if err != nil {
		log.Printf("database connection failed: %v", err)
		return err
	}

	http.HandleFunc("/schedule", s.handleScheduleTask)
	http.HandleFunc("/status", s.handleTaskStatus)

	s.httpServer = &http.Server{
		Addr: s.serverPort,
	}

	log.Printf("Starting scheduler server on %s\n", s.serverPort)

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %s\n", err)
		}
	}()

	return s.awaitShutdown()
}

func (s *SchedulerServer) handleScheduleTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Only POST request is allowerd", http.StatusMethodNotAllowed)
		return
	}

	var commandRequest SchduleTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&commandRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log.Printf("Received schedule request: %+v", commandRequest)

	if commandRequest.DelaySeconds < 0 {
		http.Error(w, "delay_seconds must be >= 0", http.StatusBadRequest)
		return
	}

	if commandRequest.DelaySeconds > 86400 { // 24h max for example
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

	// jsonResponse, err := json.Marshal(commandResponse)
	// if err != nil {
	// 	http.Error(w, err.Error(), http.StatusInternalServerError)
	// }
	//
  w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(commandResponse)
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

    var task Task

    err := s.dbPool.QueryRow(
        r.Context(),
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
    json.NewEncoder(w).Encode(response)
}


func (s *SchedulerServer) insertIntoDB(ctx context.Context, task Task) (string, error) {
	sqlStatement := "INSERT INTO tasks (command, scheduled_at) VALUES ($1, $2) RETURNING id"

	var taskID string
	err := s.dbPool.QueryRow(ctx, sqlStatement, task.Command, task.ScheduledAt).Scan(&taskID)
	if err != nil {
		return "", err
	}

	return taskID, nil
}

func (s *SchedulerServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	return s.Stop()
}

func (s *SchedulerServer) Stop() error {
	s.dbPool.Close()
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(ctx)
	}

	log.Println("Scheduler server and database pool stopped")
	return nil
}

