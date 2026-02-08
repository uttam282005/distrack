// Package worker gets tasks to process
package worker

import (
	"context"
	"net"
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
