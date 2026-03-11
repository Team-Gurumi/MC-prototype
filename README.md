# Mutual Cloud Prototype

## Project Overview
Mutual Cloud is a decentralized orchestration framework for distributed task execution. Instead of relying on a logically centralized scheduler, Mutual Cloud combines a control server, execution agents, a distributed hash table, lease-based execution ownership, and heartbeat-driven failure detection to coordinate task execution across multiple nodes.

This repository is the cleaned prototype layout used to reflect the architecture described in the research paper. Experimental artifacts and evaluation code have been removed. The remaining code focuses on the runtime prototype only.

## Architecture Overview
Mutual Cloud separates coordination responsibilities across a small set of components.

- The control server accepts task submissions, persists job state, arbitrates leases, and collects completion results.
- Execution agents discover published tasks through DHT metadata, attempt lease acquisition, execute work, and report completion.
- The DHT distributes task metadata and task advertisements so agents can discover work without a centralized scheduler loop.
- Lease ownership ensures that only one valid executor owns a task at a time.
- Heartbeats renew lease TTLs while a task is still running.
- If heartbeats stop, the lease expires and the task can be reassigned to another agent.

## System Components

### Control Server
- Publishes task metadata and task state into the DHT.
- Stores authoritative lease and completion state in PostgreSQL.
- Arbitrates lease acquisition and heartbeat renewal.
- Collects task completion and updates final status.

### Execution Agent
- Discovers candidate tasks through DHT advertisements and task indexes.
- Attempts to claim execution ownership through the control server.
- Sends heartbeats while execution is active.
- Fetches inputs, runs the task, and reports completion.

### Distributed Hash Table
- Stores task metadata, manifests, state mirrors, and discovery indexes.
- Enables decentralized task discovery across multiple agents.
- Reduces dependence on a centralized scheduler for dispatch.

### Lease-based Execution Ownership
- Agents claim tasks through a lease acquisition step.
- Each lease has a TTL and a fencing token.
- Fencing tokens prevent stale or duplicated executors from completing old work.

### Heartbeat-based Failure Detection
- Active agents periodically renew lease expiration through heartbeat requests.
- Missing heartbeats allow leases to expire naturally.
- Expired tasks are re-queued and become discoverable again.

## Execution Workflow
The runtime follows this sequence:

1. A task is published to the control server.
2. The control server stores task metadata and advertises the task through the DHT.
3. Execution agents discover the task from DHT indexes and advertisements.
4. An agent attempts to acquire the task lease from the control server.
5. The successful agent executes the task and continues sending heartbeats.
6. On completion, the agent reports the final result to the control server.
7. The control server marks the task finished and removes the active lease.

## Failure Recovery Mechanism
Failure recovery is based on lease expiration.

- Each claimed task has a bounded lease TTL.
- While the task is running, the owning agent renews that TTL through heartbeats.
- If the agent crashes, disconnects, or stops heartbeating, the lease expires.
- Once expired, the control server re-queues the task and republishes it for discovery.
- Another agent can then claim the task and continue execution.

This design avoids leader-election-based failover in the execution path and keeps reassignment logic simple and explicit.

## Installation
Prerequisites:

- Go
- PostgreSQL
- Docker

Install dependencies and build the binaries:

```bash
go mod tidy
go build ./cmd/control
go build ./cmd/agent
go build ./cmd/seeder
```

## Running the Control Server
Set the PostgreSQL DSN and start the control server:

```bash
export MC_DB_DSN='postgres://user:password@host:5432/dbname?sslmode=disable'
go build ./cmd/control
./control
```

Optional flags:

- `--ns` to set the DHT namespace
- `--bootstrap` to join an existing DHT overlay
- `--http-port` to change the control API port

## Running an Agent
Build and start an execution agent that points to the control server:

```bash
go build ./cmd/agent
./agent --control http://CONTROL_ADDRESS:8080
```

Equivalent explicit form:

```bash
./agent --control-url http://CONTROL_ADDRESS:8080
```

Optional flags:

- `--ns` to match the control server namespace
- `--bootstrap` to join the same DHT overlay
- `--ttl-sec` to set lease TTL
- `--heartbeat-sec` to set heartbeat interval

## Example Multi-node Deployment
One control node and multiple agent nodes can share the same namespace and bootstrap overlay.

Control node:

```bash
export MC_DB_DSN='postgres://user:password@control-host:5432/mc?sslmode=disable'
go build ./cmd/control
./control --ns mc
```

Seeder node:

```bash
go build ./cmd/seeder
./seeder --ns mc --bootstrap <CONTROL_BOOTSTRAP_MULTIADDR>
```

Agent node A:

```bash
go build ./cmd/agent
./agent --ns mc --control http://CONTROL_HOST:8080 --bootstrap <CONTROL_BOOTSTRAP_MULTIADDR>
```

Agent node B:

```bash
go build ./cmd/agent
./agent --ns mc --control http://CONTROL_HOST:8080 --bootstrap <CONTROL_BOOTSTRAP_MULTIADDR>
```

With this layout, multiple agents can attach to the same control server while discovering and competing for work through shared DHT metadata and lease arbitration.

## Repository Layout
```text
cmd/
  control/    control server entrypoint
  agent/      execution agent entrypoint
  seeder/     P2P artifact seeder entrypoint

internal/
  task/       task metadata, indexing, task state, lease keys
  lease/      lease store and PostgreSQL-backed arbitration
  dht/        DHT node and JSON storage helpers
  heartbeat/  heartbeat-related constants

pkg/
  agent/      agent-side runtime helpers
  p2p/        P2P protocol definitions
  seeder/     seeder-side runtime helpers

configs/
scripts/
```
