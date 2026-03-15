package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	dhtnode "github.com/Team-Gurumi/MC/internal/dht"
	"github.com/Team-Gurumi/MC/pkg/seeder"
)

func main() {
	ns := flag.String("ns", "default", "namespace")
	bootstrap := flag.String("bootstrap", "", "comma-separated bootstrap peers")
	baseDir := flag.String("base", "./inputs", "directory to seed files from (files named by root_cid)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Parse bootstrap peer list
	var boots []string
	if *bootstrap != "" {
		for _, s := range strings.Split(*bootstrap, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				boots = append(boots, s)
			}
		}
	}

	// Start DHT/libp2p node
	node, err := dhtnode.NewNode(ctx, *ns, boots)
	if err != nil {
		log.Fatalf("[seeder] failed to create node: %v", err)
	}
	defer node.Close()

	// File source: finds files named by root_cid under inputs/
	src := seeder.SourceFS{Base: *baseDir}

	// Register P2P handler (/mc-get/1.0.0)
	seeder.MountSeedHandler(ctx, node, src)

	// Print info
	fmt.Println("[seeder] PeerID:", node.Host.ID())
	for _, a := range node.Multiaddrs() {
		fmt.Println("[seeder] addr:", a)
	}
	fmt.Printf("[seeder] serving files under %s (name == root_cid)\n", *baseDir)
	fmt.Println("[seeder] register this PeerID/addr in providers[] when posting manifest")

	// Keep alive
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// keep alive
		}
	}
}
