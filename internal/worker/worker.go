// Package worker gets tasks to process
package worker

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/uttam282005/distrack/proto"
	"google.golang.org/grpc"
)

const (
	DefaultHeartBeat int = 3
)

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer

	serverPort               string
	coordinatorAddress       string
	coordinatorServiceClient *pb.CoordinatorServiceClient
	cooridnatorServiceConn   *grpc.ClientConn
	heartbeatInterval        time.Duration
	workerID                 uuid.UUID
	taskQueue                chan *pb.TaskRequest
	address                  string
	wg                       sync.WaitGroup
	ctx                      context.Context
	cancel                   context.CancelFunc
	listener                 net.Listener
	grpcServer               grpc.Server
}

func NewServer(port string, coordinator string) *WorkerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerServer{
		workerID:           uuid.New(),
		serverPort:         port,
		coordinatorAddress: coordinator,
		heartbeatInterval:  time.Duration(DefaultHeartBeat) * time.Second,
		taskQueue:          make(chan *pb.TaskRequest, 100),
		ctx:                ctx,
		cancel:             cancel,
	}
}

func(w *WorkerServer) setUpWorkerPool(workerCount int) {
	for range workerCount {
		w.wg.Add(1)
		go w.worker()
	}
}

func(w *WorkerServer) SubmitTask(ctx context.Context, task *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("Received task: %+v", task)

	w.taskQueue <- task

	return &pb.TaskResponse{
		Message: "Task was submitted",
		Success: true,
		TaskId: task.TaskId,
	}, nil
}

func(w *WorkerServer) worker() {
	defer w.wg.Done()

	for {
		select {
		case task := <-w.taskQueue:
			go updateTaskStatus(pb.TaskStatus_INPROGRESS)
			processTask(task)
		  updateTaskStatus(pb.TaskStatus_COMPLETED)
		case <-w.ctx.Done():
			return
		}
	}
}

func(w *WorkerServer) processTask(task *pb.TaskRequest) {
	commandString := task.Data
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	file, err := os.Create(fmt.Sprintf("%s_output.txt", w.workerID))
	if (err != nil) {
		log.Printf("worker: failed to create output file.")
		return;
	}
	cmd := exec.CommandContext(ctx, commandString)
	cmd.Stdout = file
	cmd.Stderr = file

	_, err = cmd.CombinedOutput()
}

