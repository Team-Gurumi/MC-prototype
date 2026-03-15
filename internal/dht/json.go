package dht

import (
	"encoding/json"
	"strings"
	"sync"
	"time"
)

// Package-level local store (fallback for demo / single-node mode)
var localStore sync.Map // key(string nsKey) -> []byte

// PutJSON: marshal to JSON, then Put with 5s timeout.
// Falls back to localStore when network PutValue fails (e.g. zero peers).
func (n *Node) PutJSON(key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := n.withTimeout(5 * time.Second) // withTimeout은 node.go에 이미 있음
	defer cancel()

	if err := n.DHT.PutValue(ctx, n.nsKey(key), b); err != nil {
		// Single node / no peers → local store fallback
		if strings.Contains(err.Error(), "failed to find any peer in table") {
			localStore.Store(n.nsKey(key), b)
			return nil
		}
		return err
	}
	return nil
}
func (n *Node) DelJSON(key string) error {
	// 1) Write an empty object as a tombstone to the network.
	if err := n.PutJSON(key, struct{}{}); err != nil {
		return err
	}
	// 2) Also remove from the local fallback store if present.
	localStore.Delete(n.nsKey(key))
	return nil
}

// GetJSON: falls back to localStore when network GetValue fails.
func (n *Node) GetJSON(key string, out any, timeout time.Duration) error {
	ctx, cancel := n.withTimeout(timeout)
	defer cancel()

	var data []byte
	v, err := n.DHT.GetValue(ctx, n.nsKey(key))
	if err != nil {
		if lv, ok := localStore.Load(n.nsKey(key)); ok {
			data = lv.([]byte)
		} else {
			return err
		}
	} else {
		data = v
	}
	return json.Unmarshal(data, out)
}
