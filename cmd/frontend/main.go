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

	"github.com/alicebob/miniredis/v2"
	"github.com/kelseyhightower/envconfig"
	"github.com/redis/rueidis"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sync/errgroup"

	"github.com/castaneai/arena"
	"github.com/castaneai/arena/arenaredis"
	"github.com/castaneai/arena/gen/arena/arenaconnect"
)

type config struct {
	Port           string `envconfig:"PORT" default:"8080"`
	RedisAddr      string `envconfig:"REDIS_ADDR"`
	RedisKeyPrefix string `envconfig:"REDIS_KEY_PREFIX"`
}

func (c *config) RedisClient() (rueidis.Client, error) {
	addr := c.RedisAddr
	if addr == "" {
		r, err := miniredis.Run()
		if err != nil {
			return nil, fmt.Errorf("failed to run miniredis: %w", err)
		}
		addr = r.Addr()
	}
	return rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true})
}

func main() {
	var conf config
	envconfig.MustProcess("", &conf)
	slog.Info(fmt.Sprintf("arena frontend config: %+v", conf))

	redisClient, err := conf.RedisClient()
	if err != nil {
		err := fmt.Errorf("failed to create redis client: %w", err)
		slog.Error(err.Error(), "error", err)
		os.Exit(1)
	}
	roomAllocator := arenaredis.NewRoomAllocator(conf.RedisKeyPrefix, redisClient)
	frontend := arena.NewFrontendService(roomAllocator)

	mux := http.NewServeMux()
	mux.Handle(arenaconnect.NewFrontendServiceHandler(frontend))
	handler := h2c.NewHandler(mux, &http2.Server{})
	addr := fmt.Sprintf(":%s", conf.Port)
	server := &http.Server{Addr: addr, Handler: handler}

	ctx, shutdown := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer shutdown()
	eg := &errgroup.Group{}
	eg.Go(func() error {
		slog.Info(fmt.Sprintf("arena frontend service is listening on %s...", addr))
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
