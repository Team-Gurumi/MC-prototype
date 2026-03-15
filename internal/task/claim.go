package task

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	dhtnode "github.com/Team-Gurumi/MC/internal/dht"
)

var (
	ErrLeaseBusy   = errors.New("lease busy")
	ErrLeaseStolen = errors.New("lease stolen by another peer")
)

const DefaultLeaseTTL = 20 * time.Second

func randNonce() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Claim: preempt only if no lease exists or the existing lease has expired
func Claim(d *dhtnode.Node, taskID, myPeer string, ttl time.Duration) (string, error) {
	if ttl <= 0 {
		ttl = DefaultLeaseTTL
	}
	nonce := randNonce()
	key := KeyLease(taskID)

	check := func(prev []byte) (bool, []byte, error) {
		now := time.Now().UTC()

		var old Lease
		if len(prev) > 0 {
			if err := json.Unmarshal(prev, &old); err != nil {
				// Corrupted value; overwrite with a new lease
				old = Lease{}
			}
		}

		// Previous lease is still alive; task is occupied
		if old.Owner != "" && !old.Expires.IsZero() && old.Expires.After(now) {
			return false, nil, ErrLeaseBusy
		}

		newL := Lease{
			Owner:   myPeer,
			Nonce:   nonce,
			Expires: now.Add(ttl),
			Version: old.Version + 1, // fencing token
		}
		next, err := json.Marshal(newL)
		if err != nil {
			return false, nil, err
		}
		return true, next, nil
	}

	if err := d.PutJSONCAS(key, check); err != nil {
		return "", err
	}
	return nonce, nil
}

// Heartbeat: extend lease only if I am the current owner
func Heartbeat(d *dhtnode.Node, taskID, myPeer, myNonce string, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = DefaultLeaseTTL
	}
	key := KeyLease(taskID)
	return d.PutJSONCAS(key, func(prev []byte) (bool, []byte, error) {
		now := time.Now().UTC()

		var cur Lease
		if len(prev) == 0 {
			// No lease exists; nothing to renew
			return false, nil, nil
		}
		if err := json.Unmarshal(prev, &cur); err != nil {
			return false, nil, err
		}

		// Not my lease; extension denied
		if cur.Owner != myPeer || cur.Nonce != myNonce {
			return false, nil, ErrLeaseStolen
		}

		cur.Expires = now.Add(ttl)
		cur.Version++

		next, _ := json.Marshal(cur)
		return true, next, nil
	})
}

// Release: intentionally relinquish a lease I own
func Release(d *dhtnode.Node, taskID, myPeer, myNonce string) error {
	key := KeyLease(taskID)
	return d.PutJSONCAS(key, func(prev []byte) (bool, []byte, error) {
		var cur Lease
		if len(prev) == 0 {
			return false, nil, nil
		}
		if err := json.Unmarshal(prev, &cur); err != nil {
			return false, nil, err
		}
		if cur.Owner != myPeer || cur.Nonce != myNonce {
			return false, nil, ErrLeaseStolen
		}

		cur.Owner = ""
		cur.Nonce = ""
		cur.Expires = time.Time{}
		cur.Version++

		next, _ := json.Marshal(cur)
		return true, next, nil
	})
}
