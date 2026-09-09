package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/uttam282005/distrack/internal/db"
	"github.com/uttam282005/distrack/internal/scheduler"
	"github.com/uttam282005/distrack/internal/telemetry"
)

var (
	schedulerPort = flag.String("scheduler_port", ":8081", "Port on which the Scheduler serves requests.")
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize OpenTelemetry tracer provider
	_, shutdown, err := telemetry.InitTracer(ctx, "distrack-scheduler")
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

	logger := telemetry.InitLogger("distrack-scheduler")
	dbConnectionString := db.GetDBConnectionString()

	schedulerServer := scheduler.NewServer(*schedulerPort, dbConnectionString, logger)
	if err := schedulerServer.Start(); err != nil {
		logger.Error("Scheduler server encountered fatal error", "error", err.Error())
		log.Fatalf("Error while starting server: %+v", err)
	}
}
