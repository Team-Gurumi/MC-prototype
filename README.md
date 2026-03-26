# Mutual Cloud Prototype

## Project Overview
Mutual Cloud is a decentralized orchestration framework for distributed task execution. Instead of relying on a logically centralized scheduler, Mutual Cloud combines a control server, execution agents, a distributed hash table, lease-based execution ownership, and heartbeat-driven failure detection to coordinate task execution across multiple nodes. This repository is the cleaned prototype layout used to reflect the architecture described in the research paper. 

## Architecture Overview

```mermaid
graph TD

Client[Client] -->|Submit task| Control[Control Server]

Control -->|Persist task state| ManagementAgent[Management Agent]
Control -->|Lease arbitration| ManagementAgent
Control -->|Publish metadata| DHT[DHT Overlay<br/>discovery]

Agent[Execution Agents] -.->|Task discovery| DHT
Agent -->|Lease claim| Control
Agent -->|Heartbeat / Finish| Control
```

Mutual Cloud separates coordination responsibilities across a small set of components.

- The control server accepts task submissions, persists job state, arbitrates leases, and collects completion results.
- Execution agents discover published tasks through DHT metadata, attempt lease acquisition, execute work, and report completion.
- The DHT distributes task metadata, task indexes, manifests, and task advertisements so agents can discover work without a centralized scheduler loop.
- Lease ownership ensures that only one valid executor owns a task at a time.
- Heartbeats renew lease TTLs while a task is still running.
- If heartbeats stop, the lease expires and the task can be reassigned to another agent.

## System Components

### Control Server
- Publishes task metadata and task state into the DHT.
- Stores authoritative lease and completion state in Management Agent. Lease arbitration is performed using transaction operations to ensure that only one valid executor owns a task at a time.
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
```mermaid
sequenceDiagram
    participant Client
    participant Control as Control Server
    participant ManagementAgent as Management Agent
    participant DHT as DHT Overlay
    participant Agent

    Client->>Control: Submit task
    Control->>ManagementAgent: Persist task state
    Control->>DHT: Publish metadata
    Agent-->>DHT: Discover task
    Agent->>Control: TryClaim(TaskID)
    Control->>ManagementAgent: Lease arbitration (CAS)
    ManagementAgent-->>Control: Claim granted
    Control-->>Agent: Claim granted

    loop Execution
        Agent->>Control: Heartbeat
        Control->>ManagementAgent: Renew lease TTL
    end

    Agent->>Control: Finish(Result)
    Control->>ManagementAgent: Finalize state
    Control->>DHT: Update status
```

The runtime follows this sequence:

1. A task is published to the control server.
2. The control server stores task metadata and advertises the task through the DHT.
3. Execution agents discover the task from DHT indexes and advertisements.
4. An agent attempts to acquire the task lease from the control server.
5. The successful agent executes the task and continues sending heartbeats.
6. On completion, the agent reports the final result to the control server.
7. The control server marks the task finished and removes the active lease.

## Failure Recovery Mechanism
```mermaid
sequenceDiagram
    participant AgentA
    participant AgentB
    participant Control
    participant ManagementAgent

    AgentA->>Control: Heartbeat
    Control->>ManagementAgent: Renew lease TTL
    Note over AgentA: Agent A crashes
    Note over AgentA,Control: Heartbeats stop
    Control->>ManagementAgent: Scan for expired leases (requeueLoop)
    ManagementAgent-->>Control: Task lease expired
    Control->>ManagementAgent: Requeue task
    AgentB->>Control: TryClaim
    Control->>ManagementAgent: Lease arbitration
    ManagementAgent-->>Control: Claim granted
    Control-->>AgentB: Claim granted
```

Failure recovery is based on lease expiration.

- Each claimed task has a bounded lease TTL.
- While the task is running, the owning agent renews that TTL through heartbeats.
- If the agent crashes, disconnects, or stops heartbeating, the lease expires.
- Once expired, the control server re-queues the task and republishes it for discovery.
- Another agent can then claim the task and continue execution.

This design avoids leader-election-based failover in the execution path and keeps reassignment logic simple and explicit.

## Mapping to Paper Architecture
The research paper describes three logical roles:

- Control Server
- Execution Agent
- Management Agent

In the prototype implementation, the responsibilities of the Management Agent are implemented inside the control server for simplicity.

| Paper Role | Implementation |
|---|---|
| Control Server | `cmd/control` |
| Execution Agent | `cmd/agent` |
| Management Agent | control server runtime + `internal/lease` |

## System Properties
Mutual Cloud provides the following guarantees:

- At-most-one active executor per task via lease arbitration.
- Automatic task recovery through lease expiration.
- Decentralized task discovery using DHT metadata and task indexes.
- Fault tolerance without centralized scheduler dispatch loops.

## Installation
Prerequisites:

- Go 1.24 or later
- PostgreSQL (used as the authoritative state backend)
- Docker (optional, required for container-based task execution)

Set up the database schema before first use:

```bash
psql -f configs/schema.sql <YOUR_POSTGRES_DSN>
```

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
export MC_DB_DSN='postgres://user:pass@host:5432/dbname?sslmode=disable'
go build ./cmd/control
./control
```

Optional flags:

- `--ns` to set the DHT namespace (default: `mc`)
- `--bootstrap` to join an existing DHT overlay
- `--http-port` to change the control API port (default: `8080`)
- `--create` to create tasks at startup (comma-separated task IDs)
- `--image` to set the container image for created tasks (default: `alpine`)
- `--cmd` to set the command for created tasks (comma-separated, default: `echo,hello`)

## Running an Agent
Build and start an execution agent that points to the control server:

```bash
go build ./cmd/agent
./agent --control-url http://CONTROL_ADDRESS:8080
```

`--control` is accepted as a shorthand alias for `--control-url`.

Optional flags:

- `--ns` to match the control server namespace (default: `default`)
- `--bootstrap` to join the same DHT overlay
- `--ttl-sec` to set lease TTL in seconds (default: `15`)
- `--heartbeat-sec` to set heartbeat interval in seconds (default: `5`)
- `--discover-every` to set task discovery interval (default: `5s`)
- `--auth-token` to set the Control API authentication token (can also be set via `CONTROL_TOKEN` env var)

## Example Multi-node Deployment
One control node and multiple agent nodes can share the same namespace and bootstrap overlay.

Control node:

```bash
export MC_DB_DSN='<STATE_BACKEND_DSN>'
go build ./cmd/control
./control --ns mc
```

Seeder node:

```bash
go build ./cmd/seeder
./seeder --ns mc --bootstrap <CONTROL_BOOTSTRAP_MULTIADDR>
```

Optional seeder flags:

- `--base` to set the directory to seed files from (default: `./inputs`)

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
```
cmd/
  control/      control server entrypoint
  agent/        execution agent entrypoint
  seeder/       P2P artifact seeder entrypoint

internal/
  task/         task metadata, indexing, task state, lease keys
  lease/        lease store and authoritative state management
  dht/          DHT node and JSON storage helpers
  heartbeat/    heartbeat-related constants

pkg/
  agent/        agent-side runtime helpers
  p2p/          P2P protocol definitions
  seeder/       seeder-side runtime helpers

configs/
  schema.sql    PostgreSQL schema for the demand_jobs table

docs/
  architecture.mmd   Mermaid source for the architecture diagram

scripts/
Makefile              remote build and deployment helpers
```
