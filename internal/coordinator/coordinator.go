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
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultMaxMisses = 3
	scanInterval     = common.DefaultHeartbeat * defaultMaxMisses
)

type CoordinatorServer struct {
	pb.UnimplementedCoordinatorServiceServer

	serverPort          string
	listener            net.Listener
	grpcServer          *grpc.Server
	WorkerPool          map[string]*WorkerInfo
	WorkerPoolMutex     sync.Mutex
	WorkerPoolKeys      []string
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
	workerServiceClient pb.WorkerServiceClient
}

func NewServer(port string, dbConnectionString string) *CoordinatorServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &CoordinatorServer{
		WorkerPool:         make(map[string]*WorkerInfo),
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

func (c *CoordinatorServer) scanDatabase() {
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			go c.executeAllScheduledTasks()
		case <-c.ctx.Done():
			log.Println("Shutting down database scanner.")
			return
		}
	}
}

func (c *CoordinatorServer) executeAllScheduledTasks() {
}

func (c *CoordinatorServer) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartBeatResponse, error) {
	c.WorkerPoolMutex.Lock()
	defer c.WorkerPoolMutex.Unlock()

	workerID := req.GetWorkerId()
	if worker, ok := c.WorkerPool[workerID]; ok {
		worker.heartbeatMisses = 0
	} else {
		conn, err := grpc.NewClient(req.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("failed to connect to the worker: %s\n", workerID)
		}
		worker := &WorkerInfo{
			address:             req.GetAddress(),
			conn:                conn,
			heartbeatMisses:     0,
			workerServiceClient: pb.NewWorkerServiceClient(conn),
		}

		c.WorkerPool[workerID] = worker
		c.regenWorkerKeys()
	}

	return &pb.HeartBeatResponse{
		Acknowledged: true,
	}, nil
}

func (c *CoordinatorServer) regenWorkerKeys() {
	c.WorkerPoolKeysMutex.Lock()
	defer c.WorkerPoolKeysMutex.Unlock()

	workerCount := len(c.WorkerPool)
	c.WorkerPoolKeys = make([]string, workerCount)

	for workerID := range c.WorkerPool {
		c.WorkerPoolKeys = append(c.WorkerPoolKeys, workerID)
	}
}

func (c *CoordinatorServer) manageWorkerPool() {
	ticker := time.Tick(scanInterval * time.Second)

	for {
		select {
		case <-ticker:
			c.removeStaleWorkers()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *CoordinatorServer) removeStaleWorkers() {
	c.WorkerPoolMutex.Lock()
	defer c.WorkerPoolMutex.Unlock()

	for workerID, worker := range c.WorkerPool {
		if worker.heartbeatMisses > defaultMaxMisses {
			worker.conn.Close()
			delete(c.WorkerPool, workerID)
			c.regenWorkerKeys()
		} else {
			worker.heartbeatMisses++
		}
	}
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
				log.Printf("failed to close connection for worker %s: %v", id, err)
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

func (c *CoordinatorServer) UpdateTaskStatus(ctx context.Context, req *pb.UpdateStatusRequest) (*pb.UpdateStatusResponse, error) {
	taskID := req.GetTaskId()
	status := req.GetStatus()
	var field string
	var value int64

	switch status {
	case pb.TaskStatus_COMPLETED:
		field = "completed_at"
		value = req.GetCompletedAt()

	case pb.TaskStatus_FAILED:
		field = "failed_at"
		value = req.GetFailedAt()

	case pb.TaskStatus_INPROGRESS:
		field = "started_at"
		value = req.GetStartedAt()
	}

	timestamp := time.Unix(value, 0)
	sqlStatement := fmt.Sprintf("update tasks set %s=$1 where id=$2", field)
	_, err := c.dbPool.Exec(ctx, sqlStatement, timestamp, taskID)
	if err != nil {
		log.Printf("Could not update task status for task %s: %+v", taskID, err)
		return nil, err
	}

	return &pb.UpdateStatusResponse{Success: true}, nil
}
