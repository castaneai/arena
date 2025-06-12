# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Arena is a Go library for managing room allocations in multiplayer games. It provides an abstraction layer for allocating game rooms across containers (processes/pods) organized into fleets.

Key concepts:
- **Room**: Single game session location
- **Container**: Holds multiple rooms (OS process/Kubernetes Pod)  
- **Fleet**: Group of containers
- **Backend**: Container-side interface for registration and room lifecycle
- **Frontend**: Client-side interface for room allocation and messaging

## Architecture

The project uses a Redis-based implementation (`arenaredis` package) with:
- `redisBackend`: Implements `Backend` interface for container management
- `redisFrontend`: Implements `Frontend` interface for room allocation
- Event-driven communication via Redis pub/sub channels
- Lua scripts for atomic Redis operations
- OpenTelemetry integration for metrics and tracing

Core interfaces are defined in root package files:
- `backend.go`: Container registration and room release
- `frontend.go`: Room allocation and messaging
- `errors.go`: Custom error types with status codes

## Development Commands

### Testing
```bash
go test ./...                    # Run all tests
go test ./arenaredis            # Test Redis implementation
```

### Load Testing
```bash
cd loadtest
go run loadtest.go             # Run load test (requires Redis)
docker-compose up -d           # Start Redis via Docker
```

### Building
```bash
go build ./...                 # Build all packages
go mod tidy                   # Clean up dependencies
```

## Key Files

- `arenaredis/backend.go`: Redis backend implementation with Lua scripts
- `arenaredis/frontend.go`: Redis frontend implementation  
- `arenaredis/container.go`: Container state management
- `arenaredis/pubsub.go`: Event pub/sub handling
- `arenaredis/metrics.go`: OpenTelemetry metrics
- `loadtest/loadtest.go`: Load testing utility

## Dependencies

- Redis client: `github.com/redis/rueidis`
- Testing: `github.com/stretchr/testify`
- Telemetry: OpenTelemetry libraries
- UUIDs: `github.com/google/uuid`