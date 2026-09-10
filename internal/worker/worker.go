// Package worker gets tasks to process
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/uttam282005/distrack/internal/telemetry"
	pb "github.com/uttam282005/distrack/proto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	DefaultHeartBeat      = 3
	defaultWorkerPoolSize = 10
)

var ErrWorkerQueueFull = status.Error(grpccodes.ResourceExhausted, "worker queue full")

type queuedTaskItem struct {
	task        *pb.TaskRequest
	traceparent string
}

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer

	serverPort               string
	coordinatorAddress       string
	coordinatorServiceClient pb.CoordinatorServiceClient
	cooridnatorServiceConn   *grpc.ClientConn
	heartbeatInterval        time.Duration
	workerID                 uuid.UUID
	taskQueue                chan *queuedTaskItem
	address                  string
	wg                       sync.WaitGroup
	ctx                      context.Context
	cancel                   context.CancelFunc
	listener                 net.Listener
	grpcServer               *grpc.Server
	logger                   *slog.Logger
	tracer                   trace.Tracer
	metrics                  *telemetry.WorkerMetrics
}

func NewServer(port string, coordinator string, logger *slog.Logger) *WorkerServer {
	if logger == nil {
		logger = telemetry.InitLogger("distrack-worker")
	}
	ctx, cancel := context.WithCancel(context.Background())
	metrics, _ := telemetry.NewWorkerMetrics()
	return &WorkerServer{
		workerID:           uuid.New(),
		serverPort:         port,
		coordinatorAddress: coordinator,
		heartbeatInterval:  time.Duration(DefaultHeartBeat * time.Second),
		taskQueue:          make(chan *queuedTaskItem, 100),
		ctx:                ctx,
		cancel:             cancel,
		logger:             logger,
		tracer:             otel.Tracer("distrack-worker"),
		metrics:            metrics,
	}
}

func (w *WorkerServer) sendHeartbeat() error {
	workerAddress := os.Getenv("WORKER_ADDRESS")
	if workerAddress == "" {
		workerAddress = w.listener.Addr().String()
	} else {
		workerAddress += w.serverPort
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := w.coordinatorServiceClient.SendHeartbeat(ctx, &pb.HeartbeatRequest{
		WorkerId: fmt.Sprintf("%v", w.workerID),
		Address:  workerAddress,
	})
	if err != nil {
		w.metrics.RecordHeartbeatSent(ctx, "failed")
	} else {
		w.metrics.RecordHeartbeatSent(ctx, "success")
	}
	return err
}

func (w *WorkerServer) closeGRPCConnection() {
	if w.grpcServer != nil {
		w.grpcServer.GracefulStop()
	}

	if w.listener != nil {
		if err := w.listener.Close(); err != nil {
			w.logger.Warn("Error while closing the listener", "error", err.Error())
		}
	}

	if w.cooridnatorServiceConn != nil {
		if err := w.cooridnatorServiceConn.Close(); err != nil {
			w.logger.Warn("Error while closing client connection with coordinator", "error", err.Error())
		}
	}
}

func (w *WorkerServer) Start() error {
	w.SetUpWorkerPool(defaultWorkerPoolSize)

	err := w.ConnectToCoordinator()
	if err != nil {
		return fmt.Errorf("failed to connect to the coordinator: %w", err)
	}
	defer w.closeGRPCConnection()

	go w.periodicHeartbeat()

	if err := w.startGRPCServer(); err != nil {
		return fmt.Errorf("failed to start worker grpc server: %w", err)
	}

	return w.awaitAndStop()
}

func (w *WorkerServer) awaitAndStop() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	return w.Stop()
}

func (w *WorkerServer) Stop() error {
	w.cancel()
	w.wg.Wait()
	w.closeGRPCConnection()
	w.logger.Info("Worker server stopped", "worker_id", w.workerID.String())
	return nil
}

func (w *WorkerServer) startGRPCServer() error {
	var err error

	if w.serverPort == "" {
		w.listener, err = net.Listen("tcp", ":0")
		w.serverPort = fmt.Sprintf(":%d", w.listener.Addr().(*net.TCPAddr).Port)
	} else {
		w.listener, err = net.Listen("tcp", w.serverPort)
	}

	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", w.serverPort, err)
	}

	w.logger.Info("Starting worker gRPC server", "port", w.serverPort, "worker_id", w.workerID.String())
	w.grpcServer = grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	pb.RegisterWorkerServiceServer(w.grpcServer, w)

	go func() {
		if err := w.grpcServer.Serve(w.listener); err != nil {
			w.logger.Error("Worker gRPC server failed", "error", err.Error())
		}
	}()

	return nil
}

func (w *WorkerServer) ConnectToCoordinator() error {
	conn, err := grpc.NewClient(
		w.coordinatorAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to coordinator: %w", err)
	}

	w.cooridnatorServiceConn = conn
	w.coordinatorServiceClient = pb.NewCoordinatorServiceClient(conn)
	w.logger.Info("Connected to coordinator", "address", w.coordinatorAddress)

	return nil
}

func (w *WorkerServer) periodicHeartbeat() {
	w.wg.Add(1)
	defer w.wg.Done()

	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := w.sendHeartbeat(); err != nil {
				w.logger.Warn("Failed to send heartbeat to coordinator", "error", err.Error())
				return
			}
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *WorkerServer) SetUpWorkerPool(workerPoolSize int) {
	for range workerPoolSize {
		w.wg.Add(1)
		go w.worker()
	}
}

func (w *WorkerServer) SubmitTask(ctx context.Context, task *pb.TaskRequest) (*pb.TaskResponse, error) {
	// Extract incoming W3C traceparent injected by coordinator's otelgrpc client handler
	tp := telemetry.InjectTraceparent(ctx)
	w.logger.InfoContext(ctx, "Received task submission",
		"task_id", task.GetTaskId(),
		"command", task.GetData(),
	)

	item := &queuedTaskItem{
		task:        task,
		traceparent: tp,
	}

	select {
	case w.taskQueue <- item:
		w.metrics.AddQueueDepth(ctx, 1)
		return &pb.TaskResponse{
			Message: "Task was submitted",
			Success: true,
			TaskId:  task.GetTaskId(),
		}, nil

	default:
		w.logger.WarnContext(ctx, "Worker task queue full, rejecting task", "task_id", task.GetTaskId())
		return nil, ErrWorkerQueueFull
	}
}

func (w *WorkerServer) worker() {
	defer w.wg.Done()

	for {
		select {
		case item := <-w.taskQueue:
			task := item.task
			taskCtx := telemetry.ExtractTraceparent(context.Background(), item.traceparent)
			w.metrics.AddQueueDepth(taskCtx, -1)
			taskCtx, span := w.tracer.Start(taskCtx, "worker.execute_task",
				trace.WithAttributes(
					attribute.String("task.id", task.GetTaskId()),
					attribute.String("task.command", task.GetData()),
					attribute.String("worker.id", w.workerID.String()),
				),
			)

			w.updateTaskStatus(taskCtx, task, pb.TaskStatus_INPROGRESS)

			w.metrics.AddActiveTasks(taskCtx, 1)
			execStart := time.Now()
			err := w.processTask(taskCtx, task)
			execDuration := time.Since(execStart).Seconds()
			w.metrics.AddActiveTasks(taskCtx, -1)

			if err != nil {
				w.metrics.RecordTaskCompletion(taskCtx, "command", "failed", execDuration)
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				span.End()
				w.updateTaskStatus(taskCtx, task, pb.TaskStatus_FAILED)
				continue
			}

			w.metrics.RecordTaskCompletion(taskCtx, "command", "success", execDuration)
			span.End()
			w.updateTaskStatus(taskCtx, task, pb.TaskStatus_COMPLETED)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *WorkerServer) updateTaskStatus(ctx context.Context, task *pb.TaskRequest, status pb.TaskStatus) {
	updateTaskStatusRequest := pb.UpdateStatusRequest{
		TaskId: task.GetTaskId(),
		Status: status,
	}

	subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := w.coordinatorServiceClient.UpdateTaskStatus(
		subCtx,
		&updateTaskStatusRequest,
	)
	if err != nil {
		w.logger.ErrorContext(ctx, "Failed to send task status update to coordinator",
			"task_id", task.GetTaskId(),
			"status", status.String(),
			"error", err.Error(),
		)
	}
}

//TODO: need isolated execution (maybe docker)
func (w *WorkerServer) processTask(ctx context.Context, task *pb.TaskRequest) error {
	ctx, span := w.tracer.Start(ctx, "worker.exec_command",
		trace.WithAttributes(
			attribute.String("task.command", task.GetData()),
			attribute.String("task.id", task.GetTaskId()),
			attribute.String("worker.id", w.workerID.String()),
		),
	)
	defer span.End()

	execCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	outputPath := fmt.Sprintf("/app/output/%s_%s.txt",
		w.workerID,
		task.GetTaskId(),
	)

	w.logger.InfoContext(ctx, "Starting command subprocess execution",
		"task_id", task.GetTaskId(),
		"command", task.GetData(),
		"output_path", outputPath,
	)

	// Ensure output directory exists
	_ = os.MkdirAll("/app/output", 0755)

	file, err := os.Create(outputPath)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		w.logger.ErrorContext(ctx, "Failed to create output file", "error", err.Error(), "path", outputPath)
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	cmd := exec.CommandContext(execCtx, "bash", "-c", task.GetData())
	cmd.Stdout = file
	cmd.Stderr = file

	err = cmd.Run()

	if execCtx.Err() == context.DeadlineExceeded {
		span.RecordError(execCtx.Err())
		span.SetStatus(codes.Error, "task timed out")
		w.logger.ErrorContext(ctx, "Task execution timed out after 4 seconds", "task_id", task.GetTaskId())
		return fmt.Errorf("task timed out")
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if exitErr, ok := err.(*exec.ExitError); ok {
			w.logger.ErrorContext(ctx, "Task execution failed with non-zero exit code",
				"task_id", task.GetTaskId(),
				"exit_code", exitErr.ExitCode(),
			)
			return fmt.Errorf("task failed with exit code %d", exitErr.ExitCode())
		}
		w.logger.ErrorContext(ctx, "Task execution system error", "task_id", task.GetTaskId(), "error", err.Error())
		return fmt.Errorf("execution error: %w", err)
	}

	w.logger.InfoContext(ctx, "Task execution completed successfully", "task_id", task.GetTaskId())
	return nil
}
