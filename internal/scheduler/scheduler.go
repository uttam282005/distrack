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
)

type SchduleTaskRequest struct {
	Command     string `json:"command"`
	ScheduledAt string `json:"scheduled_at"` // ISO 8601 format
}

type SchduleTaskResponse struct {
	Success bool   `json:"success"`
	TaskID  string `json:"taskId"`
}

type Task struct {
	Id          string
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
	s.dbPool, err = s.connectToDatabase(s.ctx)
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
		if err := s.httpServer.ListenAndServe(); err != nil && er != http.ErrServerClosed {
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

	var (
		commandRequest  SchduleTaskRequest
		commandResponse SchduleTaskResponse
	)
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

	commandResponse.TaskID = taskID
	commandResponse.Success = true

	jsonResponse, err := json.Marshal(commandResponse)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.Write(jsonResponse)
}

func (s *SchedulerServer) handleTaskStatus() {
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

func (s *SchedulerServer) connectToDatabase(ctx context.Context) (*pgxpool.Pool, error) {
	var dbPool *pgxpool.Pool
	var err error
	retryCount := 0
	for retryCount < 5 {
		dbPool, err = pgxpool.New(ctx, s.dbConnectionString)
		if err == nil {
			break
		}
		log.Printf("Failed to connect to the database. Retrying in 5 seconds...")
		time.Sleep(5 * time.Second)
		retryCount++
	}

	if err != nil {
		log.Printf("Ran out of retries to connect to database (5)")
		return nil, err
	}

	log.Printf("Connected to the database.")
	return dbPool, nil
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
