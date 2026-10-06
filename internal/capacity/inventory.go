package capacity

import (
	"sync"

	"github.com/wyw14/cry-103/internal/topology"
)

// PathInventory records which repeaters are provisioned on each span's primary
// and protection paths. The estimator cannot compare path headroom without
// knowing the repeater chain that belongs to each path.
type PathInventory struct {
	mu     sync.RWMutex
	chains map[string]map[topology.Path][]string
}

func NewPathInventory() *PathInventory {
	return &PathInventory{chains: make(map[string]map[topology.Path][]string)}
}

// Assign replaces the repeater chain of a span/path. Repeater order is kept as
// provisioning order (shore to shore), though the estimate is order
// independent because the weakest unit dominates.
func (i *PathInventory) Assign(spanID string, path topology.Path, repeaterIDs ...string) {
	if spanID == "" || (path != topology.PathPrimary && path != topology.PathProtection) {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.chains[spanID] == nil {
		i.chains[spanID] = make(map[topology.Path][]string)
	}
	chain := append([]string(nil), repeaterIDs...)
	i.chains[spanID][path] = chain
}

// Repeaters returns the provisioned repeater IDs for a span/path.
func (i *PathInventory) Repeaters(spanID string, path topology.Path) []string {
	i.mu.RLock()
	defer i.mu.RUnlock()
	chain := i.chains[spanID][path]
	return append([]string(nil), chain...)
}
