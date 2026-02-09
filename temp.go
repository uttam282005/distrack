package main

import (
	"fmt"

	pb "github.com/uttam282005/distrack/proto"
)

func main() {
	s := pb.TaskStatus_COMPLETED
	fmt.Print(s.String())
}
