// Package coordinator
package coordinator

import (
	"context"
	"net"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/uttam282005/distrack/proto"
	"google.golang.org/grpc"
)

const (
	allowedHeartbearMisses = 3
)

type CoordinatorServerService struct {
	pb.UnimplementedCoordinatorServiceServer

	serverPort string
	dbPool     *pgxpool.Pool
	server     *grpc.Server
	listener   net.Listener
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}
