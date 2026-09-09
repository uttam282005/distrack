package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/uttam282005/distrack/internal/telemetry"
	"github.com/uttam282005/distrack/internal/worker"
)

var (
	serverPort      = flag.String("worker_port", ":8000", "Port on which the Worker serves requests.")
	coordinatorPort = flag.String("coordinator", ":8080", "Network address of the Coordinator.")
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize OpenTelemetry tracer provider
	_, shutdown, err := telemetry.InitTracer(ctx, "distrack-worker")
	if err != nil {
		log.Printf("Warning: failed to initialize telemetry tracer: %v", err)
	} else {
		defer func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			if err := shutdown(shutdownCtx); err != nil {
				log.Printf("Error shutting down tracer: %v", err)
			}
		}()
	}

	logger := telemetry.InitLogger("distrack-worker")

	workerServer := worker.NewServer(*serverPort, *coordinatorPort, logger)
	if err := workerServer.Start(); err != nil {
		logger.Error("Worker server encountered fatal error", "error", err.Error())
		log.Fatalf("Error while starting server: %+v", err)
	}
}
