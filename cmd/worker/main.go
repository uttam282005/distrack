package main

import (
	"flag"
	"log"

	"github.com/uttam282005/distrack/internal/worker"
)

var (
	serverPort      = flag.String("worker_port", ":8000", "Port on which the Worker serves requests.")
	coordinatorPort = flag.String("coordinator", ":8080", "Network address of the Coordinator.")
)

func main() {
	flag.Parse()

	worker := worker.NewServer(*serverPort, *coordinatorPort)
	if err := worker.Start(); err != nil {
		log.Fatalf("Error while starting server: %+v", err)
	}
}
