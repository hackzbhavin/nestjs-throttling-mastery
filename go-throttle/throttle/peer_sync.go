package throttle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// PeerSync gossips local bucket state between the 2 servers in the same DC.
//
// Every 100ms it:
//   1. Snapshots local token counts.
//   2. POSTs them to each peer's /internal/throttle-sync.
//   3. Receives the peer's snapshot.
//   4. Merges using minimum-wins (MergePeer).
//
// Convergence time: 100ms (1 cycle).
// Overshoot window: at most 100ms worth of requests — acceptable.
//
// On failure: silent. The Manager tracks consecutive failures
// and transitions to LOCAL mode after 5 misses.
type PeerSync struct {
	local       *LocalBucket
	peerURLs    []string
	internalKey string
	client      *http.Client
	onFailure   func()  // called by Manager to count failures
	onSuccess   func()  // called by Manager to reset failure count
}

func NewPeerSync(
	local *LocalBucket,
	peerURLs []string,
	internalKey string,
	onFailure func(),
	onSuccess func(),
) *PeerSync {
	return &PeerSync{
		local:       local,
		peerURLs:    peerURLs,
		internalKey: internalKey,
		client:      &http.Client{Timeout: 50 * time.Millisecond}, // hard 50ms — don't add latency
		onFailure:   onFailure,
		onSuccess:   onSuccess,
	}
}

// Start runs the sync loop. Call in a goroutine.
func (ps *PeerSync) Start() {
	if len(ps.peerURLs) == 0 {
		slog.Warn("peer sync: no peer URLs configured — running in single-node mode")
		return
	}
	slog.Info("peer sync started", "peers", ps.peerURLs, "interval", "100ms")
	ticker := time.NewTicker(100 * time.Millisecond)
	for range ticker.C {
		ps.syncOnce()
	}
}

func (ps *PeerSync) syncOnce() {
	snap := ps.local.Snapshot()
	body, _ := json.Marshal(snap)

	for _, peerURL := range ps.peerURLs {
		peerSnap, err := ps.callPeer(peerURL, body)
		if err != nil {
			// Don't log every miss — floods logs during outage
			ps.onFailure()
			continue
		}
		ps.local.MergePeer(peerSnap)
		ps.onSuccess()
	}
}

func (ps *PeerSync) callPeer(peerURL string, body []byte) (map[string]float64, error) {
	req, err := http.NewRequest(http.MethodPost,
		fmt.Sprintf("%s/internal/throttle-sync", peerURL),
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Key", ps.internalKey)

	resp, err := ps.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer returned %d", resp.StatusCode)
	}

	var peerSnap map[string]float64
	if err := json.NewDecoder(resp.Body).Decode(&peerSnap); err != nil {
		return nil, err
	}
	return peerSnap, nil
}
