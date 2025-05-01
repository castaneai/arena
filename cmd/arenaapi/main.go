package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os/signal"
	"sync"
	"syscall"

	"connectrpc.com/connect"
	"connectrpc.com/validate"
	"github.com/alicebob/miniredis/v2"
	"github.com/kelseyhightower/envconfig"
	"github.com/redis/rueidis"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/castaneai/arena/arenaconnect"
	"github.com/castaneai/arena/arenaconnect/castaneai/arena/v1/arenav1connect"
	"github.com/castaneai/arena/arenaredis"
)

type config struct {
	ListenPort     string `envconfig:"PORT" default:"8080"`
	RedisAddr      string `envconfig:"REDIS_ADDR"`
	RedisKeyPrefix string `envconfig:"REDIS_KEY_PREFIX" default:"arena:"`
}

func main() {
	var conf config
	envconfig.MustProcess("ARENA", &conf)
	slog.Info(fmt.Sprintf("starting arena API server with config: %+v", conf))

	ctx, shutdown := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer shutdown()

	// init arena Frontend/Backend with redis
	redis, err := newRedisClient(&conf)
	if err != nil {
		log.Fatalf("failed to create redis client: %v", err)
	}
	frontend := arenaredis.NewFrontend(conf.RedisKeyPrefix, redis)
	backend := arenaredis.NewBackend(ctx, conf.RedisKeyPrefix, redis)

	// init connect-RPC server
	mux := http.NewServeMux()
	interceptor, err := validate.NewInterceptor()
	if err != nil {
		log.Fatalf("failed to create interceptor: %v", err)
	}
	mux.Handle(arenav1connect.NewFrontendServiceHandler(arenaconnect.NewFrontendService(frontend), connect.WithInterceptors(interceptor)))
	mux.Handle(arenav1connect.NewBackendServiceHandler(arenaconnect.NewBackendService(backend), connect.WithInterceptors(interceptor)))
	handler := h2c.NewHandler(mux, &http2.Server{})
	addr := fmt.Sprintf(":%s", conf.ListenPort)
	server := &http.Server{Addr: addr, Handler: handler}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = server.ListenAndServe()
	}()
	slog.Info(fmt.Sprintf("server listening on %s... ", addr))
	<-ctx.Done()
	slog.Info("shutting down server...")
	wg.Wait()
}

func newRedisClient(conf *config) (rueidis.Client, error) {
	redisConf := rueidis.ClientOption{
		InitAddress:  []string{conf.RedisAddr},
		DisableCache: true,
	}
	if conf.RedisAddr == "" {
		mr, err := miniredis.Run()
		if err != nil {
			return nil, fmt.Errorf("failed to run miniredis: %w", err)
		}
		redisConf.InitAddress = []string{mr.Addr()}
	}
	return rueidis.NewClient(redisConf)
}
