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

	"github.com/jackc/pgx/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttam282005/distrack/internal/db"
)

type SchduleTaskRequest struct {
	Command     string `json:"command"`
	ScheduledAt string `json:"scheduled_at"` // ISO 8601 format
}

type TaskResponse struct {
	TaskID      string `json:"task_id"`
	Command     string `json:"command"`
	ScheduledAt string `json:"scheduled_at,omitempty"`
	PickedAt    string `json:"picked_at,omitempty"`
	StartedAt   string `json:"started_at,omitempty"`
	CompletedAt string `json:"completed_at,omitempty"`
	FailedAt    string `json:"failed_at,omitempty"`
}

type Task struct {
	ID          string
	Command     string
	ScheduledAt pgtype.Timestamp
	PickedAt    pgtype.Timestamp
	StartedAt   pgtype.Timestamp
	CompletedAt pgtype.Timestamp
	FailedAt    pgtype.Timestamp
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

	scheduledTime, err := time.Parse(time.RFC3339, commandRequest.ScheduledAt)
	if err != nil {
		http.Error(w, "Invalid date format. Use ISO 8601 format.", http.StatusBadRequest)
		return
	}

	unixTimestamp := time.Unix(scheduledTime.Unix(), 0)

	task := Task{
		Command:     commandRequest.Command,
		ScheduledAt: pgtype.Timestamp{Time: unixTimestamp},
	}
	taskID, err := s.insertIntoDB(context.Background(), task)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to submit task. Error: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	commandResponse := TaskResponse{
		TaskID:      taskID,
		Command:     commandRequest.Command,
		ScheduledAt: commandRequest.ScheduledAt,
		FailedAt:    "",
		CompletedAt: "",
		StartedAt:   "",
	}
	jsonResponse, err := json.Marshal(commandResponse)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.Write(jsonResponse)
}

func (s *SchedulerServer) handleTaskStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Only POST request is allowerd", http.StatusMethodNotAllowed)
		return
	}

	taskID := r.URL.Query().Get("task_id")
	if taskID == "" {
		http.Error(w, "Task ID is required", http.StatusBadRequest)
		return
	}

	var task Task
	err := s.dbPool.QueryRow(context.Background(), "SELECT * FROM tasks WHERE id = $1", taskID).Scan(&task.ID, &task.Command, &task.ScheduledAt, &task.PickedAt, &task.StartedAt, &task.CompletedAt, &task.FailedAt)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get task status. Error: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	taskResponse := TaskResponse{
		TaskID: task.ID,
		Command: task.Command,
		PickedAt: "",
		ScheduledAt: "",
		FailedAt: "",
		CompletedAt: "",
	}

	// Set the schedule_at time if non-null
if task.ScheduledAt.Status == pgtype.Present {
	taskResponse.ScheduledAt = task.ScheduledAt.Time.String()
	}

	// Set the picked_at time if non-null.
	if task.PickedAt.Status == pgtype.Present {
		taskResponse.PickedAt = task.PickedAt.Time.String()
	}

	// Set the started_at time if non-null.
	if task.StartedAt.Status == pgtype.Present {
		taskResponse.StartedAt = task.StartedAt.Time.String()
	}

	// Set the completed_at time if non-null.
	if task.CompletedAt.Status == pgtype.Present {
		taskResponse.CompletedAt = task.CompletedAt.Time.String()
	}

	// Set the failed_at time if non-null.
	if task.FailedAt.Status == pgtype.Present {
		taskResponse.FailedAt = task.FailedAt.Time.String()
	}

	// Convert the taskResponse struct to JSON
	jsonResponse, err := json.Marshal(taskResponse)
	if err != nil {
		http.Error(w, "Failed to marshal JSON response", http.StatusInternalServerError)
		return
	}

	// Set the Content-Type header to application/json
	w.Header().Set("Content-Type", "application/json")

	// Write the JSON taskResponse
	w.Write(jsonResponse)
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
