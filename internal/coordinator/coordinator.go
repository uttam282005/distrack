// Package coordinator
package coordinator

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttam282005/distrack/internal/common"
	"github.com/uttam282005/distrack/internal/db"
	pb "github.com/uttam282005/distrack/proto"
	"google.golang.org/grpc"
)

const (
	defaultMaxMisses = 3
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

func NewServer(port string, dbConnectionString string) *CoordinatorServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &CoordinatorServer{
		WorkerPool:         make(map[uint32]*WorkerInfo),
		maxHeartbeatMisses: defaultMaxMisses,
		heartbeatInterval:  common.DefaultHeartbeat,
		dbConnectionString: dbConnectionString,
		serverPort:         port,
		ctx:                ctx,
		cancel:             cancel,
	}
}

func (c *CoordinatorServer) Start() error {
	var err error
	go c.manageWorkerPool()

	if err = c.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server start failed: %w", err)
	}

	c.dbPool, err = db.ConnectToDatabase(c.ctx, c.dbConnectionString)
	if err != nil {
		return err
	}

	go c.scanDatabase()

	return c.awaitShutdown()
}

func (c *CoordinatorServer) manageWorkerPool() {
}

func (c *CoordinatorServer) startGRPCServer() error {
	var err error

	if c.serverPort == "" {
		// Find a free port using a temporary socket
		c.listener, err = net.Listen("tcp", ":0")                                // Bind to any available port
		c.serverPort = fmt.Sprintf(":%d", c.listener.Addr().(*net.TCPAddr).Port) // Get the assigned port
	} else {
		c.listener, err = net.Listen("tcp", c.serverPort)
	}

	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", c.serverPort, err)
	}

	log.Printf("Starting worker server on %s\n", c.serverPort)
	c.grpcServer = grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(c.grpcServer, c)

	go func() {
		if err := c.grpcServer.Serve(c.listener); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	return nil
}

func (c *CoordinatorServer) scanDatabase() {
}

func (c *CoordinatorServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	return c.Stop()
}

func (c *CoordinatorServer) Stop() error {
	c.cancel()
	c.wg.Wait()

	c.WorkerPoolMutex.Lock()
	defer c.WorkerPoolMutex.Unlock()

	for id, worker := range c.WorkerPool {
		if worker.conn != nil {
			if err := worker.conn.Close(); err != nil {
				log.Printf("failed to close connection for worker %d: %v", id, err)
			}
		}
	}

	if c.grpcServer != nil {
		c.grpcServer.GracefulStop()
	}

	if c.listener != nil {
		if err := c.listener.Close(); err != nil {
			return fmt.Errorf("failed to close coordinator listener: %w", err)
		}
	}

	c.dbPool.Close()

	return nil
}
