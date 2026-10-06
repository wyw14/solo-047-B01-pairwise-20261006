package api

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/capacity"
	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/incident"
	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/otdr"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/repair"
	"github.com/wyw14/cry-103/internal/repeater"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

type Services struct {
	Clock               platform.Clock
	IDs                 platform.IDGenerator
	Journal             *store.Journal
	Spans               map[string]*span.Span
	Routes              map[string]*route.Route
	Repeaters           map[string]*repeater.Repeater
	Topology            *topology.Registry
	OTDR                *otdr.SessionService
	Switchovers         *switchover.SessionService
	Permits             *repair.PermitService
	Receipts            *landing.ReceiptRegistry
	Acceptance          *repair.AcceptanceService
	Telemetry           *telemetry.Ingestor
	TelemetryRepository *telemetry.Repository
	Admission           *incident.AdmissionService
	Classifier          *incident.Classifier
	Correlator          *otdr.Correlator
	Health              *landing.HealthService
	Station             *landing.Station
	WetPlantTests       *repair.TestService
	Ramps               *repeater.RampController
	Assets              *repair.Assets
	RepeaterHealth      *repeater.HealthProjector
	Capacity            *capacity.Service
	PathInventory       *capacity.PathInventory
	Actuator            *feed.Actuator
	Tests               map[string]*repair.TestSession
	Incidents           map[string]*incident.Incident
	mu                  sync.RWMutex
}

func (s *Services) Span(id string) (*span.Span, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	target := s.Spans[id]
	if target == nil {
		return nil, fmt.Errorf("span %s not found", id)
	}
	return target, nil
}

func (s *Services) Route(id string) (*route.Route, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	target := s.Routes[id]
	if target == nil {
		return nil, fmt.Errorf("route %s not found", id)
	}
	return target, nil
}

func (s *Services) Repeater(id string) (*repeater.Repeater, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	target := s.Repeaters[id]
	if target == nil {
		return nil, fmt.Errorf("repeater %s not found", id)
	}
	return target, nil
}

func (s *Services) Persist(kind, entityID string, value any) error {
	return s.Journal.Append(kind, entityID, value, s.Clock.Now())
}

func (s *Services) StartWetPlantTest(ctx context.Context, spanID string) repair.TestSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.WetPlantTests.Start(ctx, spanID)
	snapshot := s.WetPlantTests.Snapshot(session)
	s.Tests[snapshot.ID] = session
	return snapshot
}

func (s *Services) FinishWetPlantTest(id, action string) (repair.TestSnapshot, error) {
	s.mu.Lock()
	session := s.Tests[id]
	delete(s.Tests, id)
	s.mu.Unlock()
	if session == nil {
		return repair.TestSnapshot{}, fmt.Errorf("wet-plant test %s not found", id)
	}
	if action == "complete" {
		return s.WetPlantTests.Complete(session), nil
	}
	return s.WetPlantTests.Cancel(session), nil
}

func (s *Services) Incident(id, spanID string) *incident.Incident {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.Incidents[id]
	if current == nil {
		current = incident.New(id, spanID, s.Clock.Now().Add(10*time.Second), s.Clock.Now())
		s.Incidents[id] = current
	}
	return current
}

func (s *Services) TelemetryHistory(assetID string) []telemetry.Sample {
	items := s.TelemetryRepository.History(assetID)
	if latest, ok := s.TelemetryRepository.Latest(assetID, telemetry.KindTemperature); ok {
		_ = latest
	}
	return items
}

// SpanSnapshot implements capacity.SnapshotSource.
func (s *Services) SpanSnapshot(id string) (span.Snapshot, bool) {
	target, err := s.Span(id)
	if err != nil || target == nil {
		return span.Snapshot{}, false
	}
	return target.Snapshot(), true
}

// RouteSnapshot implements capacity.SnapshotSource.
func (s *Services) RouteSnapshot(id string) (route.Snapshot, bool) {
	target, err := s.Route(id)
	if err != nil || target == nil {
		return route.Snapshot{}, false
	}
	return target.Snapshot(), true
}

// TopologySnapshot implements capacity.SnapshotSource.
func (s *Services) TopologySnapshot(spanID string) (topology.Snapshot, bool) {
	if s.Topology == nil {
		return topology.Snapshot{}, false
	}
	snapshot, err := s.Topology.Snapshot(spanID)
	if err != nil {
		return topology.Snapshot{}, false
	}
	return snapshot, true
}

// RepeaterSnapshot implements capacity.SnapshotSource.
func (s *Services) RepeaterSnapshot(id string) (repeater.Snapshot, bool) {
	target, err := s.Repeater(id)
	if err != nil || target == nil {
		return repeater.Snapshot{}, false
	}
	return target.Snapshot(), true
}

// LatestErrorRate implements capacity.SnapshotSource. The returned sample
// carries its own generation, which the estimator binds to the topology.
func (s *Services) LatestErrorRate(repeaterID string) (telemetry.Sample, bool) {
	if s.TelemetryRepository == nil {
		return telemetry.Sample{}, false
	}
	return s.TelemetryRepository.Latest(repeaterID, telemetry.KindErrorRate)
}

// AssignPathRepeaters binds a repeater chain to a span path so the capacity
// estimator knows which repeaters belong to primary versus protection.
func (s *Services) AssignPathRepeaters(spanID string, path topology.Path, repeaterIDs []string) error {
	if s.PathInventory == nil {
		return fmt.Errorf("capacity path inventory is not configured")
	}
	if _, ok := s.Spans[spanID]; !ok {
		return fmt.Errorf("span %s not found", spanID)
	}
	if path != topology.PathPrimary && path != topology.PathProtection {
		return fmt.Errorf("unsupported path %q", path)
	}
	for _, repeaterID := range repeaterIDs {
		if _, ok := s.Repeaters[repeaterID]; !ok {
			return fmt.Errorf("repeater %s not registered", repeaterID)
		}
	}
	s.PathInventory.Assign(spanID, path, repeaterIDs...)
	return nil
}
