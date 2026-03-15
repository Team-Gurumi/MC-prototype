package seeder

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"

	dhtnode "github.com/Team-Gurumi/MC/internal/dht"

	"github.com/Team-Gurumi/MC/pkg/p2p"
	"github.com/libp2p/go-libp2p/core/network"
)

// Source interface: opens a file by rootCID.
type Source interface {
	Open(rootCID string) (io.ReadCloser, int64, error)
}

// Opens a file named after rootCID in the local directory.
type SourceFS struct{ Base string }

func (s SourceFS) Open(cid string) (io.ReadCloser, int64, error) {
	path := filepath.Join(s.Base, cid)
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	st, _ := f.Stat()
	return f, st.Size(), nil
}

// Register a libp2p stream handler: receive a JSON request, send file bytes directly.
func MountSeedHandler(ctx context.Context, n *dhtnode.Node, src Source) {
	n.Host.SetStreamHandler(p2p.ProtoGet, func(s network.Stream) {
		defer s.Close()

		// 1) Read JSON request
		var req struct {
			RootCID string `json:"root_cid"`
		}
		if err := json.NewDecoder(bufio.NewReader(s)).Decode(&req); err != nil {
			log.Printf("[seeder] bad json: %v", err)
			return
		}
		if req.RootCID == "" {
			log.Printf("[seeder] empty root_cid")
			return
		}

		// 2) Open file
		rc, size, err := src.Open(req.RootCID)
		if err != nil {
			log.Printf("[seeder] open %s: %v", req.RootCID, err)
			return
		}
		defer rc.Close()

		// 3) Stream file body
		bw := bufio.NewWriter(s)
		written, err := io.Copy(bw, rc)
		if err != nil {
			log.Printf("[seeder] copy err: %v", err)
			return
		}
		if err := bw.Flush(); err != nil {
			log.Printf("[seeder] flush err: %v", err)
			return
		}
		log.Printf("[seeder] sent %d/%d bytes for %s", written, size, req.RootCID)
	})
}
