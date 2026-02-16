package main

import (
	"flag"
	"log"

	"github.com/uttam282005/distrack/internal/coordinator"
	"github.com/uttam282005/distrack/internal/db"
)

var coordinatorPort = flag.String("coordinator_port", ":8080", "Port on which the Coordinator serves requests.")

func main() {
	flag.Parse()
	dbConnectionString := db.GetDBConnectionString()
	coordinator := coordinator.NewServer(*coordinatorPort, dbConnectionString)
	if err := coordinator.Start(); err != nil {
		log.Fatalf("Error while starting server: %+v", err)
	}
}
