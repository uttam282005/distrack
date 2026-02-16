package main

import (
	"flag"
	"log"

	"github.com/uttam282005/distrack/internal/db"
	"github.com/uttam282005/distrack/internal/scheduler"
)

var (
	schedulerPort = flag.String("scheduler_port", ":8081", "Port on which the Scheduler serves requests.")
)

func main() {
	dbConnectionString := db.GetDBConnectionString()
	schedulerServer := scheduler.NewServer(*schedulerPort, dbConnectionString)
	err := schedulerServer.Start()
	if err != nil {
		log.Fatalf("Error while starting server: %+v", err)
	}
}
