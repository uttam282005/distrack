// Package coordinator
package coordinator

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/uttam282005/distrack/proto"
	"google.golang.org/grpc"
)

type CoordinatorServer struct {
	pb.UnimplementedCoordinatorServiceServer

	serverPort          string
	listener            net.Listener
	grpcServer          *grpc.Server
	WorkerPool          map[uint32]*WorkerInfo
	WorkerPoolMutex     sync.Mutex
	WorkerPoolKeys      []uint32
	WorkerPoolKeysMutex sync.RWMutex
	maxHeartbeatMisses  uint8
	heartbeatInterval   time.Duration
	roundRobinIndex     uint32
	dbConnectionString  string
	dbPool              *pgxpool.Pool
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
}

type WorkerInfo struct {
	heartbeatMisses     uint8
	conn                *grpc.ClientConn
	address             string
	workerServiceClient *pb.WorkerServiceClient
}
