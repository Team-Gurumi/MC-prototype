package agent

import (
	"time"

	task "github.com/Team-Gurumi/MC/internal/task"
)

// Must match the advertisement payload that Control publishes to the DHT
type TaskAd struct {
	JobID     string    `json:"job_id"`
	Namespace string    `json:"ns,omitempty"`
	DemandURL string    `json:"demand_url,omitempty"`
	Topic     string    `json:"topic,omitempty"`
	Exp       time.Time `json:"exp"`
	Sig       string    `json:"sig,omitempty"`
}

type ManifestAd struct {
	RootCID    string          `json:"root_cid"`
	Providers  []task.Provider `json:"providers"`
	Rendezvous string          `json:"rendezvous,omitempty"`
	Transports []string        `json:"transports,omitempty"`
	Exp        time.Time       `json:"exp"`
}

// DHT key convention (same as control server)
func KeyTaskAd(ns, id string) string        { return "ad/" + ns + "/task/" + id }
func KeyP2PManifestMirror(id string) string { return "p2p/" + id + "/manifest" }
