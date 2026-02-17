// Package worker gets tasks to process
package worker

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	pb "github.com/uttam282005/distrack/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	DefaultHeartBeat      = 3
	defaultWorkerPoolSize = 5
)

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer

	serverPort               string
	coordinatorAddress       string
	coordinatorServiceClient pb.CoordinatorServiceClient
	cooridnatorServiceConn   *grpc.ClientConn
	heartbeatInterval        time.Duration
	workerID                 uuid.UUID
	taskQueue                chan *pb.TaskRequest
	address                  string
	wg                       sync.WaitGroup
	ctx                      context.Context
	cancel                   context.CancelFunc
	listener                 net.Listener
	grpcServer               *grpc.Server
}

func NewServer(port string, coordinator string) *WorkerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerServer{
		workerID:           uuid.New(),
		serverPort:         port,
		coordinatorAddress: coordinator,
		heartbeatInterval:  time.Duration(DefaultHeartBeat * time.Second),
		taskQueue:          make(chan *pb.TaskRequest, 100),
		ctx:                ctx,
		cancel:             cancel,
	}
}

func (w *WorkerServer) sendHeartbeat() error {
	workerAddress := os.Getenv("WORKER_ADDRESS")
	if workerAddress == "" {
		// Fall back to using the listener address if WORKER_ADDRESS is not set
		workerAddress = w.listener.Addr().String()
	} else {
		workerAddress += w.serverPort
	}

	_, err := w.coordinatorServiceClient.SendHeartbeat(context.Background(), &pb.HeartbeatRequest{
		WorkerId: fmt.Sprintf("%v", w.workerID),
		Address:  workerAddress,
	})
	return err
}

func (w *WorkerServer) closeGRPCConnection() {
	if w.grpcServer != nil {
		w.grpcServer.GracefulStop()
	}

	if w.listener != nil {
		if err := w.listener.Close(); err != nil {
			log.Printf("Error while closing the listener: %v", err)
		}
	}

	if err := w.cooridnatorServiceConn.Close(); err != nil {
		log.Printf("Error while closing client connection with coordinator: %v", err)
	}
}

func (w *WorkerServer) Start() error {
	w.SetUpWorkerPool(defaultWorkerPoolSize)

	err := w.ConnectToCoordinator()
	if err != nil {
		return fmt.Errorf("failed to connect to the coordinator")
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
	log.Println("Worker server stopped")
	return nil
}

func (w *WorkerServer) startGRPCServer() error {
	var err error

	if w.serverPort == "" {
		// Find a free port using a temporary socket
		w.listener, err = net.Listen("tcp", ":0")                                // Bind to any available port
		w.serverPort = fmt.Sprintf(":%d", w.listener.Addr().(*net.TCPAddr).Port) // Get the assigned port
	} else {
		w.listener, err = net.Listen("tcp", w.serverPort)
	}

	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", w.serverPort, err)
	}

	log.Printf("Starting worker server on %s\n", w.serverPort)
	w.grpcServer = grpc.NewServer()
	pb.RegisterWorkerServiceServer(w.grpcServer, w)

	go func() {
		if err := w.grpcServer.Serve(w.listener); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	return nil
}

func (w *WorkerServer) ConnectToCoordinator() error {
	conn, err := grpc.NewClient(
		w.coordinatorAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to coordinator: %w", err)
	}

	w.cooridnatorServiceConn = conn
	w.coordinatorServiceClient = pb.NewCoordinatorServiceClient(conn)

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
				log.Printf("Failed to send heartbeat: %v", err)
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
	log.Printf("Received task: %+v", task)

	w.taskQueue <- task

	return &pb.TaskResponse{
		Message: "Task was submitted",
		Success: true,
		TaskId:  task.TaskId,
	}, nil
}

func (w *WorkerServer) worker() {
	defer w.wg.Done()

	for {
		select {
		case task := <-w.taskQueue:
			go w.updateTaskStatus(task, pb.TaskStatus_INPROGRESS)

			if err := w.processTask(task); err != nil {
				w.updateTaskStatus(task, pb.TaskStatus_FAILED)
				continue
			}

			w.updateTaskStatus(task, pb.TaskStatus_COMPLETED)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *WorkerServer) updateTaskStatus(task *pb.TaskRequest, status pb.TaskStatus) {
	updateTaskStatusRequest := pb.UpdateStatusRequest{
		TaskId: task.GetTaskId(),
		Status: status,
	}

	// TODO: send with exponential backoff
	_, err := w.coordinatorServiceClient.UpdateTaskStatus(
		context.Background(),
		&updateTaskStatusRequest,
	)
	if err != nil {
		log.Printf("failed to send task update to coordinator")
	}
}

func (w *WorkerServer) processTask(task *pb.TaskRequest) error {
    ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
    defer cancel()

    outputPath := fmt.Sprintf("/app/output/%s_%s.txt",
        w.workerID,
        task.GetTaskId(),
    )

    file, err := os.Create(outputPath)
    if err != nil {
        return fmt.Errorf("failed to create output file: %w", err)
    }
    defer file.Close()

    cmd := exec.CommandContext(ctx, "bash", "-c", task.GetData())
    cmd.Stdout = file
    cmd.Stderr = file

    err = cmd.Run()

    if ctx.Err() == context.DeadlineExceeded {
        return fmt.Errorf("task timed out")
    }

    if err != nil {
        if exitErr, ok := err.(*exec.ExitError); ok {
            return fmt.Errorf("task failed with exit code %d", exitErr.ExitCode())
        }
        return fmt.Errorf("execution error: %w", err)
    }

    return nil
}
