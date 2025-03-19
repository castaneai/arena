package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"connectrpc.com/connect"
	"github.com/kelseyhightower/envconfig"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sync/errgroup"

	"github.com/castaneai/arena/gen/arena"
	"github.com/castaneai/arena/gen/arena/arenaconnect"
)

type config struct {
	Port string `envconfig:"PORT" default:"8080"`
}

func main() {
	var conf config
	envconfig.MustProcess("", &conf)

	mux := http.NewServeMux()
	mux.Handle(arenaconnect.NewFrontendServiceHandler(&frontendService{}))
	handler := h2c.NewHandler(mux, &http2.Server{})
	addr := fmt.Sprintf(":%s", conf.Port)
	server := &http.Server{Addr: addr, Handler: handler}

	ctx, shutdown := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer shutdown()
	eg := &errgroup.Group{}
	eg.Go(func() error {
		slog.Info(fmt.Sprintf("frontend service (Connect RPC) is listening on %s...", addr))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	<-ctx.Done()
	slog.Info("shutting down arena frontend server...")
	if err := server.Shutdown(context.Background()); err != nil {
		err := fmt.Errorf("failed to shutdown arena frontend server: %w", err)
		slog.Error(err.Error(), "error", err)
	}
	_ = eg.Wait()
}

type frontendService struct{}

func (s *frontendService) AllocateRoom(ctx context.Context, req *connect.Request[arena.AllocateRoomRequest]) (*connect.Response[arena.AllocateRoomResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("implement me"))
}
