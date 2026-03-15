package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	dhtnode "github.com/Team-Gurumi/MC/internal/dht"
	"github.com/Team-Gurumi/MC/internal/task"
	"github.com/Team-Gurumi/MC/pkg/p2p"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type FetchResult struct {
	LocalPath string
	Bytes     int64
}

// Try a single provider. Returns file path and byte count on success.
func FetchFromProvider(ctx context.Context, d *dhtnode.Node, rootCID string, pv task.Provider, dir string) (*FetchResult, error) {
	if pv.PeerID == "" || len(pv.Addrs) == 0 {
		return nil, fmt.Errorf("invalid provider")
	}
	// 1) Build PeerInfo
	var addrs []ma.Multiaddr
	for _, s := range pv.Addrs {
		m, err := ma.NewMultiaddr(s)
		if err != nil {
			continue
		}
		addrs = append(addrs, m)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no valid addrs")
	}
	pi, err := peer.AddrInfoFromP2pAddr(addrs[0].Encapsulate(ma.StringCast("/p2p/" + pv.PeerID)))
	if err != nil {
		return nil, fmt.Errorf("peer info: %w", err)
	}

	// 2) Connect (short timeout)
	ctx2, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	if err := d.Host.Connect(ctx2, *pi); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	// 3) Open stream
	s, err := d.Host.NewStream(ctx, pi.ID, p2p.ProtoGet)
	if err != nil {
		return nil, fmt.Errorf("new stream: %w", err)
	}
	defer s.Close()

	// 4) Send request
	req := p2p.GetRequest{RootCID: rootCID}
	if err := json.NewEncoder(s).Encode(&req); err != nil {
		return nil, fmt.Errorf("send req: %w", err)
	}

	// 5) Destination file path
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	tmp := filepath.Join(dir, rootCID+".part")
	out, err := os.Create(tmp)
	if err != nil {
		return nil, err
	}
	defer out.Close()

	// 6) Stream body copy
	nw, err := io.Copy(out, bufio.NewReader(s))
	if err != nil {
		return nil, fmt.Errorf("recv: %w", err)
	}

	final := filepath.Join(dir, rootCID)
	if err := os.Rename(tmp, final); err != nil {
		return nil, err
	}
	if rootCID != "input" {
		inPath := filepath.Join(dir, "input")
		_ = os.RemoveAll(inPath)
		// Try symlink, fall back to hard copy on failure
		if err := os.Symlink(rootCID, inPath); err != nil {
			// Symlink blocked or Windows; fall back to copy
			if err2 := copyFile(final, inPath); err2 != nil {
				// Best-effort: even if this fails, the fetch is still successful
			}
		}
	}
	return &FetchResult{LocalPath: final, Bytes: nw}, nil
}

// Try multiple providers sequentially; return first success
func FetchAny(ctx context.Context, d *dhtnode.Node, rootCID string, providers []task.Provider, dir string) (*FetchResult, error) {
	var lastErr error
	for _, pv := range providers {
		res, err := FetchFromProvider(ctx, d, rootCID, pv, dir)
		if err == nil {
			return res, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no providers")
	}
	return nil, lastErr
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
