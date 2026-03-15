package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	dhtnode "github.com/Team-Gurumi/MC/internal/dht"
	"github.com/Team-Gurumi/MC/internal/task"
	"github.com/Team-Gurumi/MC/pkg/agent"
)

func main() {
	ns := flag.String("ns", "default", "namespace")
	discEvery := flag.Duration("discover-every", 5*time.Second, "task discovery interval")
	bootstrapPeers := flag.String("bootstrap", "", "comma-separated bootstrap peer list")

	controlURL := flag.String("control-url", "http://127.0.0.1:8080", "Control API base URL")
	controlAlias := flag.String("control", "", "alias for -control-url")
	authToken := flag.String("auth-token", "", "Control API auth token")
	ttlSec := flag.Int("ttl-sec", 15, "lease TTL seconds (recommended: 15)")
	hbSec := flag.Int("heartbeat-sec", 5, "heartbeat interval seconds (recommended: 5)")
	flag.Parse()
	if *controlAlias != "" {
		*controlURL = *controlAlias
	}

	// Main context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// DHT node
	d, err := initDHTNode(ctx, *ns, *bootstrapPeers)
	if err != nil {
		log.Fatalf("DHT node init failed: %v", err)
	}

	// Token
	var token string
	if *authToken != "" {
		token = *authToken
	} else {
		token = os.Getenv("CONTROL_TOKEN")
	}

	// TTL / heartbeat correction
	leaseTTL := time.Duration(*ttlSec) * time.Second
	hbEvery := time.Duration(*hbSec) * time.Second
	if hbEvery >= leaseTTL {
		if leaseTTL > time.Second {
			hbEvery = leaseTTL - time.Second
		} else {
			hbEvery = leaseTTL / 2
		}
	}

	log.Printf("[agent] config: TTL=%s heartbeat=%s (flags: -ttl-sec=%d -heartbeat-sec=%d)", leaseTTL, hbEvery, *ttlSec, *hbSec)

	// Clients for communicating with the control server
	claim := &agent.HTTPClaimClient{
		BaseURL: *controlURL,
		Client:  &http.Client{Timeout: 5 * time.Second},
		Token:   token,
	}
	finish := &agent.FinishClient{
		BaseURL: *controlURL,
		Client:  &http.Client{Timeout: 5 * time.Second},
		Token:   token,
	}

	agentID := d.Host.ID().String()

	// discoverer
	dv := agent.NewDiscoverer(d, *ns, *discEvery)
	listIDs := func() []string { return agent.ListFromIndex(d, *ns) }

	onCandidate := func(jobID string, providers []task.Provider, demandURL string) {
		// 1) 이 잡에 대해 사용할 베이스 URL을 정한다.
		base := *controlURL
		if demandURL != "" {
			base = demandURL
		}
		// claim 이랑 finish 둘 다 같은 베이스를 쓰도록 맞춘다.
		claim.BaseURL = base
		finish.BaseURL = base

		// 1) try-claim
		ctx2, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()

		lease, err := claim.TryClaim(ctx2, jobID, agentID, leaseTTL)
		if err != nil {

			return
		}
		if manWait, err := agent.WaitForManifest(context.Background(), *controlURL, token, jobID, 15*time.Second); err == nil {
			log.Printf("[agent] manifest ready for job=%s (root_cid=%s)", jobID, manWait.RootCID)
		} else {
			log.Printf("[agent] manifest wait timeout for job=%s: %v", jobID, err)
		}
		log.Printf("[agent] lease acquired job=%s ver=%d exp=%s",
			jobID, lease.Version, lease.Expires.Format(time.RFC3339))
		log.Printf(`{"event":"lease_acquired","timestamp":"%s","job_id":"%s","agent_id":"%s"}`,
			time.Now().UTC().Format(time.RFC3339Nano), jobID, agentID)

		leaseToken := lease.Version

		// Job-specific context
		jobCtx, cancelJob := context.WithCancel(context.Background())

		// 2) Heartbeat goroutine
		go func(taskID, nonce string, leaseTok int64) {
			defer log.Printf("[agent] job=%s heartbeat stopped", taskID)

			t := time.NewTicker(hbEvery)
			defer t.Stop()

			fail := 0
			const maxFail = 3

			for {
				select {
				case <-jobCtx.Done():
					return
				case <-t.C:
					if _, err := claim.Heartbeat(context.Background(), taskID, agentID, nonce, leaseTTL, leaseTok); err != nil {
						fail++
						if fail >= maxFail {
							return
						}
						continue
					}
					fail = 0
				}
			}
		}(jobID, lease.Nonce, leaseToken)

		// 3) Check manifest
		var man task.Manifest
		if err := d.GetJSON(task.KeyManifest(jobID), &man, 3*time.Second); err != nil || man.RootCID == "" {
			_ = finish.Report(
				context.Background(),
				jobID,
				"failed",
				map[string]any{
					"error_stage": "manifest_check",
					"reason":      "manifest_not_found",
				},
				"",
				nil,
				"manifest not found on DHT",
				agentID,
				leaseToken,
			)
			cancelJob()
			return
		}

		// 3.5) Also need to read meta for execution
		var meta task.TaskMeta
		if err := d.GetJSON(task.KeyMeta(jobID), &meta, 3*time.Second); err != nil {
			_ = finish.Report(
				context.Background(),
				jobID,
				"failed",
				map[string]any{
					"error_stage": "get_meta",
				},
				"",
				nil,
				"get meta failed: "+err.Error(),
				agentID,
				leaseToken,
			)
			cancelJob()
			return
		}

		// 4) Fetch inputs
		workDir := "./work/" + jobID
		_ = os.MkdirAll(workDir, 0o755)
		inputDir := filepath.Join(workDir, "input")
		_ = os.MkdirAll(inputDir, 0o755)

		if !strings.EqualFold(man.RootCID, "noop") && len(man.Providers) > 0 {
			if _, err := agent.FetchAny(context.Background(), d, man.RootCID, man.Providers, inputDir); err != nil {
				_ = finish.Report(
					context.Background(),
					jobID,
					"failed",
					map[string]any{
						"error_stage": "fetch_input",
					},
					"",
					nil,
					"fetch failed: "+err.Error(),
					agentID,
					leaseToken,
				)
				cancelJob()
				return
			}
		}

		// 5) Execute
		res, runErr := agent.RunInContainer(jobCtx, workDir, meta.Image, meta.Command)

		// Stop heartbeat
		cancelJob()

		status := "succeeded"
		errMsg := ""
		if runErr != nil || (res != nil && res.ExitCode != 0) {
			status = "failed"
			if runErr != nil {
				errMsg = runErr.Error()
			}
		}

		metrics := map[string]any{}
		if res != nil {
			metrics = map[string]any{
				"exit_code":    res.ExitCode,
				"duration_ms":  res.Duration.Milliseconds(),
				"stdout_bytes": len(res.Stdout),
				"stderr_bytes": len(res.Stderr),
			}
		}

		// 6) Finish report (with retries)
		const maxFinishRetries = 20              // max retry attempts
		const finishRetryDelay = 5 * time.Second // delay between each attempt

		var lastErr error
		for attempt := 1; attempt <= maxFinishRetries; attempt++ {
			err := finish.Report(
				context.Background(),
				jobID,
				status,
				metrics,
				"",
				nil,
				errMsg,
				agentID,
				leaseToken,
			)
			if err == nil {
				log.Printf("[agent] finish reported job=%s status=%s (attempt %d)", jobID, status, attempt)
				lastErr = nil
				break
			}
			lastErr = err

			if !agent.ShouldRetryFinish(err) {
				log.Printf("[agent] finish report failed (non-retryable) job=%s: %v", jobID, err)
				break
			}

			log.Printf("[agent] finish report retry %d/%d for job=%s: %v", attempt, maxFinishRetries, jobID, err)
			time.Sleep(finishRetryDelay)
		}

		if lastErr != nil {
			log.Printf("[agent] finish report failed after %d attempts job=%s: %v", maxFinishRetries, jobID, lastErr)
		}

	}

	// Run discoverer
	go dv.Run(ctx, listIDs, onCandidate)

	// Wait
	<-ctx.Done()
}

func initDHTNode(ctx context.Context, ns, bootstrapPeers string) (*dhtnode.Node, error) {
	var addrs []string
	if bootstrapPeers != "" {
		for _, s := range strings.Split(bootstrapPeers, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				addrs = append(addrs, s)
			}
		}
	}
	node, err := dhtnode.NewNode(ctx, ns, addrs)
	if err != nil {
		return nil, err
	}

	log.Printf("[agent] P2P node started: %s", node.Host.ID())
	for _, a := range node.Multiaddrs() {
		log.Printf("[agent] listening on: %s", a)
	}
	return node, nil
}
