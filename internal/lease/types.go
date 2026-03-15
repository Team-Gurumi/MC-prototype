package lease

import "time"

// Task metadata submitted via POST /api/tasks, plus creation timestamp
type JobMeta struct {
	ID         string    `json:"id"`
	Image      string    `json:"image"`
	Command    []string  `json:"command"`
	CreatedAt  time.Time `json:"createdAt"`
	RetryCount int
}

// Current state (fields already visible in API responses)
type JobState struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"` // queued|assigned|running|finished|failed
	UpdatedAt time.Time `json:"updatedAt"`
	Version   int64     `json:"version"`
	// The following 3 fields are reserved for future use (OK to leave empty for now)
	AssignedTo     string    `json:"assignedTo,omitempty"`
	LeaseExpiresAt time.Time `json:"leaseExpiresAt,omitempty"`
	FencingToken   string    `json:"fencingToken,omitempty"`
}

// Final result schema (types prepared, not yet persisted)
type MetricKV map[string]any

type Artifact struct {
	Name string `json:"name"`
	CID  string `json:"cid"`
}

type JobResult struct {
	Status        string     `json:"status"` // succeeded|failed
	FinalMetrics  MetricKV   `json:"final_metrics,omitempty"`
	ResultRootCID string     `json:"result_root_cid,omitempty"`
	Artifacts     []Artifact `json:"artifacts,omitempty"`
}

// Full Job view
type Job struct {
	Meta   *JobMeta   `json:"meta"`
	State  *JobState  `json:"state"`
	Result *JobResult `json:"result,omitempty"`
}
