package capacity

import (
	"fmt"
	"time"

	"github.com/wyw14/cry-103/internal/repeater"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

// SnapshotSource is the read model the builder assembles inputs from. The API
// Services struct satisfies it.
type SnapshotSource interface {
	SpanSnapshot(id string) (span.Snapshot, bool)
	RouteSnapshot(id string) (route.Snapshot, bool)
	TopologySnapshot(spanID string) (topology.Snapshot, bool)
	RepeaterSnapshot(id string) (repeater.Snapshot, bool)
	LatestErrorRate(repeaterID string) (telemetry.Sample, bool)
}

// Builder resolves a route to a fully-bound set of capacity Inputs using the
// path inventory to locate each path's repeater chain.
type Builder struct {
	source    SnapshotSource
	inventory *PathInventory
	now       func() time.Time
	staleness time.Duration
}

func NewBuilder(source SnapshotSource, inventory *PathInventory, now func() time.Time, staleness time.Duration) *Builder {
	return &Builder{source: source, inventory: inventory, now: now, staleness: staleness}
}

// Build collects every signal needed to budget the route's span. It never
// fabricates missing evidence; absent repeaters or BER samples are passed
// through empty so the estimator reports them as insufficient data.
func (b *Builder) Build(routeID string) (Inputs, error) {
	routeSnapshot, ok := b.source.RouteSnapshot(routeID)
	if !ok {
		return Inputs{}, fmt.Errorf("route %s not found", routeID)
	}
	spanSnapshot, ok := b.source.SpanSnapshot(routeSnapshot.SpanID)
	if !ok {
		return Inputs{}, fmt.Errorf("span %s not found", routeSnapshot.SpanID)
	}
	topologySnapshot, ok := b.source.TopologySnapshot(spanSnapshot.ID)
	if !ok {
		return Inputs{}, fmt.Errorf("topology for span %s not found", spanSnapshot.ID)
	}

	in := Inputs{
		Span:     spanSnapshot,
		Route:    routeSnapshot,
		Topology: topologySnapshot,
		Now:      b.now().UTC(),
	}
	in.Primary = b.pathSignals(spanSnapshot.ID, topology.PathPrimary)
	in.Protection = b.pathSignals(spanSnapshot.ID, topology.PathProtection)
	return in, nil
}

func (b *Builder) pathSignals(spanID string, path topology.Path) PathSignals {
	signals := PathSignals{}
	for _, repeaterID := range b.inventory.Repeaters(spanID, path) {
		if snapshot, ok := b.source.RepeaterSnapshot(repeaterID); ok {
			signals.Repeaters = append(signals.Repeaters, snapshot)
			if sample, found := b.source.LatestErrorRate(repeaterID); found {
				signals.ErrorRates = append(signals.ErrorRates, sample)
			}
		}
	}
	return signals
}
