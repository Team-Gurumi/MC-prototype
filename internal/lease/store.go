package lease

import (
	"context"
	"time"
)

// Job status managed by the DB (Demand store)
type JobStatus string

const (
	StatusQueued    JobStatus = "queued"
	StatusAssigned  JobStatus = "assigned"
	StatusRunning   JobStatus = "running"
	StatusSucceeded JobStatus = "succeeded"
	StatusFailed    JobStatus = "failed"
)

type DBJob struct {
	ID         string
	Image      string
	Command    []string
	Status     JobStatus
	CreatedAt  time.Time
	RetryCount int
}

// Manifest information
type Manifest struct {
	RootCID   string
	Providers []string
	EncMeta   string
}

type Lease struct {
	AgentID      string
	ExpireAt     time.Time
	FencingToken uint64
}

// Store interface shared by Control and Agent
type Store interface {
	// /api/tasks
	CreateJob(ctx context.Context, job DBJob) error

	// /api/tasks/{id}
	GetJob(ctx context.Context, id string) (*DBJob, error)

	// /jobs/{id}/manifest
	AttachManifest(ctx context.Context, id string, m Manifest) error

	// /api/tasks/{id}/try-claim
	TryClaim(ctx context.Context, id string, agentID string, ttl time.Duration) (*Lease, error)

	// /api/tasks/{id}/heartbeat
	Heartbeat(ctx context.Context, id string, agentID string, token uint64, ttl time.Duration) error

	// /jobs/{id}/finish
	Finish(ctx context.Context,
		id string,
		agentID string,
		token uint64,
		succeeded bool,
		resultCID string,
		artifacts []string,
		metrics any,
	) error

	// For the requeue loop
	ListExpiredLeases(ctx context.Context, now time.Time) ([]string, error)
	SetStatusQueued(ctx context.Context, id string) error
	// Used to re-advertise queued jobs when the control server restarts
	ListQueued(ctx context.Context) ([]DBJob, error)
	ListManifestMissingSince(ctx context.Context, cutoff time.Time) ([]string, error)
	ListAll(ctx context.Context) ([]DBJob, error)

	ListPaged(ctx context.Context, limit, offset int) ([]DBJob, error)

	CountByStatus(ctx context.Context) (map[JobStatus]int64, error)
}

// Common errors
var (
	ErrJobNotFound   = Err("job not found")
	ErrNoManifest    = Err("manifest not attached")
	ErrLeaseConflict = Err("lease still valid")
	ErrBadToken      = Err("fencing token mismatch")
)

type Err string

func (e Err) Error() string { return string(e) }
