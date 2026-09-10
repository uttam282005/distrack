package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/uttam282005/distrack/internal/coordinator"
	"github.com/uttam282005/distrack/internal/db"
	"github.com/uttam282005/distrack/internal/telemetry"
)

var coordinatorPort = flag.String("coordinator_port", ":8080", "Port on which the Coordinator serves requests.")

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize OpenTelemetry tracer and meter providers
	shutdown, err := telemetry.InitTelemetry(ctx, "distrack-coordinator")
	if err != nil {
		log.Printf("Warning: failed to initialize telemetry: %v", err)
	} else {
		defer func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			if err := shutdown(shutdownCtx); err != nil {
				log.Printf("Error shutting down telemetry: %v", err)
			}
		}()
	}

	logger := telemetry.InitLogger("distrack-coordinator")
	dbConnectionString := db.GetDBConnectionString()

	coordinatorServer := coordinator.NewServer(*coordinatorPort, dbConnectionString, logger)
	if err := coordinatorServer.Start(); err != nil {
		logger.Error("Coordinator server encountered fatal error", "error", err.Error())
		log.Fatalf("Error while starting server: %+v", err)
	}
}
