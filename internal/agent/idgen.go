package agent

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"sync"
)

// IDGen mints run-scoped identifiers of the form <scope>-<run nonce>-<sequence>. The
// crypto/rand run nonce makes ids unique across emulator restarts — which matters once
// ingestion keys on heartbeat event ids — while the sequence keeps them unique and ordered
// within a run. A generator is safe for concurrent use and is shared by the whole fleet.
type IDGen struct {
	mu    sync.Mutex
	nonce string
	seq   uint64
}

// NewIDGen returns an identifier generator with a fresh run nonce.
func NewIDGen() (*IDGen, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, err
	}
	return &IDGen{nonce: hex.EncodeToString(raw[:])}, nil
}

// Next returns the next identifier in the named scope. Scope is the device id for heartbeat
// event ids and a fixed label for correlation ids.
func (g *IDGen) Next(scope string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.seq++
	return scope + "-" + g.nonce + "-" + strconv.FormatUint(g.seq, 10)
}
