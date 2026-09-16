// Package coordinator
package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttam282005/distrack/internal/common"
	"github.com/uttam282005/distrack/internal/db"
	"github.com/uttam282005/distrack/internal/telemetry"
	pb "github.com/uttam282005/distrack/proto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	logger              *slog.Logger
	tracer              trace.Tracer
	metrics             *telemetry.CoordinatorMetrics
}

type WorkerInfo struct {
	heartbeatMisses     uint8
	conn                *grpc.ClientConn
	address             string
	workerServiceClient pb.WorkerServiceClient
}

func NewServer(port string, dbConnectionString string, logger *slog.Logger) *CoordinatorServer {
	if logger == nil {
		logger = telemetry.InitLogger("distrack-coordinator")
	}
	ctx, cancel := context.WithCancel(context.Background())
	metrics, _ := telemetry.NewCoordinatorMetrics()
	return &CoordinatorServer{
		WorkerPool:         make(map[string]*WorkerInfo),
		maxHeartbeatMisses: defaultMaxMisses,
		heartbeatInterval:  common.DefaultHeartbeat,
		dbConnectionString: dbConnectionString,
		serverPort:         port,
		ctx:                ctx,
		cancel:             cancel,
		logger:             logger,
		tracer:             otel.Tracer("distrack-coordinator"),
		metrics:            metrics,
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
		c.logger.Error("Database connection failed", "error", err.Error())
		return err
	}

	go c.scanDatabase()

	return c.awaitShutdown()
}

func (c *CoordinatorServer) scanDatabase() {
	ticker := time.NewTicker(scanInterval * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			go c.executeAllScheduledTasks()
		case <-c.ctx.Done():
			c.logger.Info("Shutting down database scanner.")
			return
		}
	}
}

func (c *CoordinatorServer) getNextWorker() *WorkerInfo {
	c.WorkerPoolKeysMutex.Lock()
	defer c.WorkerPoolKeysMutex.Unlock()

	workerCount := len(c.WorkerPoolKeys)
	if workerCount == 0 {
		return nil
	}

	workerID := c.WorkerPoolKeys[int(c.roundRobinIndex)%workerCount]

	c.WorkerPoolMutex.Lock()
	worker := c.WorkerPool[workerID]
	c.WorkerPoolMutex.Unlock()

	c.roundRobinIndex++

	return worker
}

func (c *CoordinatorServer) submitTaskToWorker(ctx context.Context, task *pb.TaskRequest) error {
	start := time.Now()
	worker := c.getNextWorker()
	if worker == nil {
		c.metrics.RecordTaskDispatched(ctx, "no_workers", time.Since(start).Seconds())
		return errors.New("no workers available")
	}

	subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := worker.workerServiceClient.SubmitTask(subCtx, task)
	if err != nil {
		c.metrics.RecordTaskDispatched(ctx, "error", time.Since(start).Seconds())
	} else {
		c.metrics.RecordTaskDispatched(ctx, "success", time.Since(start).Seconds())
	}
	return err
}

type scheduledTaskItem struct {
	task        *pb.TaskRequest
	traceparent string
}

func (c *CoordinatorServer) executeAllScheduledTasks() {
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	tx, err := c.dbPool.Begin(ctx)
	if err != nil {
		c.logger.ErrorContext(ctx, "Unable to start transaction for task dispatch", "error", err.Error())
		return
	}

	defer func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			c.logger.ErrorContext(ctx, "Failed to rollback transaction", "error", err.Error())
		}
	}()

	sql := "SELECT id, command, COALESCE(traceparent, '') FROM tasks WHERE scheduled_at < (NOW() + INTERVAL '30 seconds') AND picked_at IS NULL ORDER BY scheduled_at FOR UPDATE SKIP LOCKED"
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		c.logger.ErrorContext(ctx, "Error executing query for scheduled tasks", "error", err.Error())
		return
	}
	defer rows.Close()

	var items []scheduledTaskItem
	var taskIDs []string
	for rows.Next() {
		var id, command, traceparent string
		if err := rows.Scan(&id, &command, &traceparent); err != nil {
			c.logger.WarnContext(ctx, "Failed to scan task row", "error", err.Error())
			continue
		}

		items = append(items, scheduledTaskItem{
			task: &pb.TaskRequest{
				TaskId: id,
				Data:   command,
			},
			traceparent: traceparent,
		})
		taskIDs = append(taskIDs, id)
	}

	if err := rows.Err(); err != nil {
		c.logger.ErrorContext(ctx, "Error iterating task rows", "error", err.Error())
		return
	}

	if len(items) == 0 {
		return
	}

	// Batch mark all selected tasks as picked and commit immediately to release DB row locks
	if _, err := tx.Exec(ctx, `UPDATE tasks SET picked_at = NOW() WHERE id = ANY($1)`, taskIDs); err != nil {
		c.logger.ErrorContext(ctx, "Failed to batch update task picked_at", "error", err.Error())
		return
	}

	if err := tx.Commit(ctx); err != nil {
		c.logger.ErrorContext(ctx, "Failed to commit dispatch transaction", "error", err.Error())
		return
	}

	// Dispatch tasks to workers concurrently using a worker pool
	const maxDispatchWorkers = 20
	numWorkers := len(items)
	if numWorkers > maxDispatchWorkers {
		numWorkers = maxDispatchWorkers
	}

	taskCh := make(chan scheduledTaskItem, len(items))
	for _, item := range items {
		taskCh <- item
	}
	close(taskCh)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range taskCh {
				c.dispatchTask(ctx, item)
			}
		}()
	}
	wg.Wait()
}

func (c *CoordinatorServer) dispatchTask(ctx context.Context, item scheduledTaskItem) {
	// Resurrect parent trace context from PostgreSQL!
	taskCtx := telemetry.ExtractTraceparent(ctx, item.traceparent)
	taskCtx, span := c.tracer.Start(taskCtx, "coordinator.dispatch_task",
		trace.WithAttributes(
			attribute.String("task.id", item.task.GetTaskId()),
			attribute.String("task.command", item.task.GetData()),
		),
	)
	defer span.End()

	if err := c.submitTaskToWorker(taskCtx, item.task); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		c.logger.ErrorContext(taskCtx, "Failed to submit task to worker",
			"task_id", item.task.GetTaskId(),
			"error", err.Error(),
		)

		// Revert picked_at so the task can be retried on next scan
		if _, revertErr := c.dbPool.Exec(ctx, `UPDATE tasks SET picked_at = NULL WHERE id = $1`, item.task.GetTaskId()); revertErr != nil {
			c.logger.ErrorContext(taskCtx, "Failed to revert task picked_at after dispatch failure",
				"task_id", item.task.GetTaskId(),
				"error", revertErr.Error(),
			)
		}
		return
	}

	c.logger.InfoContext(taskCtx, "Dispatched task to worker", "task_id", item.task.GetTaskId())
}

func (c *CoordinatorServer) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartBeatResponse, error) {
	c.WorkerPoolMutex.Lock()
	defer c.WorkerPoolMutex.Unlock()

	workerID := req.GetWorkerId()
	if worker, ok := c.WorkerPool[workerID]; ok {
		worker.heartbeatMisses = 0
		c.metrics.RecordHeartbeat(ctx, "success")
	} else {
		conn, err := grpc.NewClient(
			req.GetAddress(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		)
		if err != nil {
			c.logger.Error("Failed to connect to worker", "worker_id", workerID, "address", req.GetAddress(), "error", err.Error())
		}
		worker := &WorkerInfo{
			address:             req.GetAddress(),
			conn:                conn,
			heartbeatMisses:     0,
			workerServiceClient: pb.NewWorkerServiceClient(conn),
		}

		c.WorkerPool[workerID] = worker
		c.regenWorkerKeys()
		c.metrics.AddActiveWorkers(ctx, 1)
		c.metrics.RecordHeartbeat(ctx, "success")
		c.logger.Info("Registered new worker in pool", "worker_id", workerID, "address", req.GetAddress())
	}

	return &pb.HeartBeatResponse{
		Acknowledged: true,
	}, nil
}

func (c *CoordinatorServer) regenWorkerKeys() {
	c.WorkerPoolKeysMutex.Lock()
	defer c.WorkerPoolKeysMutex.Unlock()

	c.WorkerPoolKeys = c.WorkerPoolKeys[:0]
	for workerID := range c.WorkerPool {
		c.WorkerPoolKeys = append(c.WorkerPoolKeys, workerID)
	}
}

func (c *CoordinatorServer) manageWorkerPool() {
	ticker := time.NewTicker(c.heartbeatInterval * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.WorkerPoolMutex.Lock()
			for id, worker := range c.WorkerPool {
				worker.heartbeatMisses++
				if worker.heartbeatMisses >= c.maxHeartbeatMisses {
					if worker.conn != nil {
						if err := worker.conn.Close(); err != nil {
							c.logger.Warn("Failed to close worker connection", "worker_id", id, "error", err.Error())
						}
					}
					delete(c.WorkerPool, id)
					c.regenWorkerKeys()
					c.metrics.AddActiveWorkers(c.ctx, -1)
					c.metrics.RecordHeartbeatFailure(c.ctx)
					c.logger.Warn("Removed dead worker from pool", "worker_id", id)
				}
			}
			c.WorkerPoolMutex.Unlock()
		case <-c.ctx.Done():
			c.logger.Info("Shutting down worker pool manager")
			return
		}
	}
}

func (c *CoordinatorServer) startGRPCServer() error {
	var err error

	if c.serverPort == "" {
		c.listener, err = net.Listen("tcp", ":0")
		c.serverPort = fmt.Sprintf(":%d", c.listener.Addr().(*net.TCPAddr).Port)
	} else {
		c.listener, err = net.Listen("tcp", c.serverPort)
	}

	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", c.serverPort, err)
	}

	c.logger.Info("Starting coordinator gRPC server", "port", c.serverPort)
	c.grpcServer = grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	pb.RegisterCoordinatorServiceServer(c.grpcServer, c)

	go func() {
		if err := c.grpcServer.Serve(c.listener); err != nil {
			c.logger.Error("gRPC server failed", "error", err.Error())
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
				c.logger.Warn("Failed to close worker connection during shutdown", "worker_id", id, "error", err.Error())
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

	if c.dbPool != nil {
		c.dbPool.Close()
	}

	c.logger.Info("Coordinator server stopped")
	return nil
}

func (c *CoordinatorServer) UpdateTaskStatus(ctx context.Context, req *pb.UpdateStatusRequest) (*pb.UpdateStatusResponse, error) {
	taskID := req.GetTaskId()
	status := req.GetStatus()

	ctx, span := c.tracer.Start(ctx, "coordinator.update_task_status",
		trace.WithAttributes(
			attribute.String("task.id", taskID),
			attribute.String("task.status", status.String()),
		),
	)
	defer span.End()

	var field string
	value := time.Now().UTC()

	switch status {
	case pb.TaskStatus_COMPLETED:
		field = "completed_at"
	case pb.TaskStatus_FAILED:
		field = "failed_at"
	case pb.TaskStatus_INPROGRESS:
		field = "started_at"
	}

	sqlStatement := fmt.Sprintf("update tasks set %s=$1 where id=$2", field)
	_, err := c.dbPool.Exec(ctx, sqlStatement, value, taskID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		c.metrics.RecordStatusUpdate(ctx, "FAILED_UPDATE")
		c.logger.ErrorContext(ctx, "Could not update task status in database",
			"task_id", taskID,
			"status", status.String(),
			"error", err.Error(),
		)
		return nil, err
	}

	c.metrics.RecordStatusUpdate(ctx, status.String())
	c.logger.InfoContext(ctx, "Task status updated in database",
		"task_id", taskID,
		"status", status.String(),
	)

	return &pb.UpdateStatusResponse{Success: true}, nil
}
