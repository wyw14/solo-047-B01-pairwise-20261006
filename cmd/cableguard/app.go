package main

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/wyw14/cry-103/internal/api"
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

func buildServices(cfg config) (*api.Services, error) {
	clock := platform.RealClock{}
	ids := platform.UUIDGenerator{}
	now := clock.Now
	snapshots := store.NewSnapshotStore(filepath.Join(cfg.Data, "snapshots"))
	checkpoints := store.NewCheckpoints(snapshots)
	journal := store.NewJournal(filepath.Join(cfg.Data, "events.jsonl"))

	topologies := topology.NewRegistry()
	if _, err := topologies.Register("S3", topology.PathPrimary, now()); err != nil {
		return nil, err
	}
	spanS3, err := span.New("S3", "South China Sea S3", 126.4, now())
	if err != nil {
		return nil, err
	}
	if _, err := spanS3.MarkCut(now()); err != nil {
		return nil, err
	}
	if _, err := spanS3.StartRepair(now()); err != nil {
		return nil, err
	}
	if _, err := spanS3.BeginProof(now()); err != nil {
		return nil, err
	}
	routeS3, err := route.New("R-S3", "S3", now())
	if err != nil {
		return nil, err
	}

	telemetryRepository := telemetry.NewRepository()
	mutes := telemetry.NewMuteManager(now)
	admission := incident.NewAdmissionService(mutes)
	repeaterIncidents := incident.NewRepeaterLifecycle()
	healthProjector := repeater.NewHealthProjector(repeaterIncidents, 85)
	repeaterR07, err := repeater.New("R07", now())
	if err != nil {
		return nil, err
	}
	healthProjector.Register(repeaterR07)
	repeaterR08, err := repeater.New("R08", now())
	if err != nil {
		return nil, err
	}
	healthProjector.Register(repeaterR08)
	ingestor := telemetry.NewIngestor(telemetryRepository, now, healthProjector)

	scanner := landing.NewScanner(topologies, 2*time.Millisecond)
	otdrService := otdr.NewSessionService(topologies, scanner, ids, now)
	drainer := route.NewDrainer(4 * time.Millisecond)
	activator := route.NewActivator(topologies)

	// Capacity budgeting. Estimates are bound to telemetry time and the
	// topology generation; the gate refuses a switch the protection path
	// cannot carry and refuses auto-expansion on insufficient evidence.
	pathInventory := capacity.NewPathInventory()
	estimator, err := capacity.NewEstimator(capacity.Config{
		Staleness:     30 * time.Second,
		LoadFactor:    0.8,
		BaseCapacity:  400_000_000_000, // 400 Gbps design capacity per lossless span
		MaxBER:        1e-12,
		HotDerate:     0.7,
		TrippedDerate: 0.0,
	})
	if err != nil {
		return nil, err
	}
	lazySource := &lazyCapacitySource{}
	capacityBuilder := capacity.NewBuilder(lazySource, pathInventory, now, 30*time.Second)
	capacityService := capacity.NewService(capacityBuilder, estimator)
	switchovers := switchover.NewSessionService(drainer, activator, capacityService, ids, now, 25*time.Millisecond)
	restorer := switchover.NewRestorer(drainer, activator, topologies, ids, now)

	receipts := landing.NewReceiptRegistry()
	isolation := feed.NewIsolationService()
	permits := repair.NewPermitService(isolation, receipts, ids, now)
	for _, circuitID := range []string{"A", "B"} {
		circuit, createErr := feed.NewCircuit(circuitID, "east", now())
		if createErr != nil {
			return nil, createErr
		}
		permits.RegisterCircuit(circuit)
	}

	acceptance := repair.NewAcceptanceService(restorer, now)
	acceptance.Register("S3", spanS3, routeS3)
	station, err := landing.NewStation("east", "East Landing", now())
	if err != nil {
		return nil, err
	}
	heartbeats := store.NewHeartbeatStore(checkpoints)
	ageEvaluator, err := telemetry.NewAgeEvaluator(now, 10*time.Second)
	if err != nil {
		return nil, err
	}
	health := landing.NewHealthService(ageEvaluator, heartbeats)
	if _, err := health.Record(station, 1, now()); err != nil {
		return nil, fmt.Errorf("seed heartbeat checkpoint: %w", err)
	}
	if _, err := health.Recover(station); err != nil {
		return nil, err
	}
	health.Evaluate(station, now())

	assets := repair.NewAssets()
	assets.AddSpan(spanS3)
	assets.AddRoute(routeS3)
	for _, circuitID := range []string{"A", "B"} {
		if circuit, ok := permitsCircuit(permits, "east", circuitID); ok {
			assets.AddCircuit(circuit)
		}
	}
	actuator := feed.NewActuator()
	ramps := repeater.NewRampController(actuator, telemetryRepository, telemetry.StabilityWindow{MinimumSamples: 2, MaximumSpread: 1.5, MinimumPeriod: time.Second}, checkpoints, now)
	wetPlantTests := repair.NewTestService(mutes, ids, now, 30*time.Second)

	services := &api.Services{
		Clock: clock, IDs: ids, Journal: journal,
		Spans: map[string]*span.Span{"S3": spanS3}, Routes: map[string]*route.Route{"R-S3": routeS3},
		Repeaters: map[string]*repeater.Repeater{"R07": repeaterR07, "R08": repeaterR08}, Topology: topologies,
		OTDR: otdrService, Switchovers: switchovers, Permits: permits, Receipts: receipts,
		Acceptance: acceptance, Telemetry: ingestor, TelemetryRepository: telemetryRepository, Admission: admission,
		Classifier: incident.NewClassifier(), Correlator: otdr.NewCorrelator(), Health: health, Station: station,
		WetPlantTests: wetPlantTests, Ramps: ramps, Assets: assets,
		RepeaterHealth: healthProjector, Capacity: capacityService, PathInventory: pathInventory, Actuator: actuator,
		Tests: make(map[string]*repair.TestSession), Incidents: make(map[string]*incident.Incident),
	}
	// The capacity builder reads live snapshots; wire it to the fully built
	// services graph and seed the primary/protection repeater chains.
	lazySource.services = services
	pathInventory.Assign("S3", topology.PathPrimary, "R07")
	pathInventory.Assign("S3", topology.PathProtection, "R08")
	return services, nil
}

// lazyCapacitySource breaks the construction cycle between the capacity
// builder (needed to build the switchover service) and api.Services (which
// holds the repositories the builder reads from).
type lazyCapacitySource struct {
	services *api.Services
}

func (s *lazyCapacitySource) SpanSnapshot(id string) (span.Snapshot, bool) {
	return s.services.SpanSnapshot(id)
}

func (s *lazyCapacitySource) RouteSnapshot(id string) (route.Snapshot, bool) {
	return s.services.RouteSnapshot(id)
}

func (s *lazyCapacitySource) TopologySnapshot(spanID string) (topology.Snapshot, bool) {
	return s.services.TopologySnapshot(spanID)
}

func (s *lazyCapacitySource) RepeaterSnapshot(id string) (repeater.Snapshot, bool) {
	return s.services.RepeaterSnapshot(id)
}

func (s *lazyCapacitySource) LatestErrorRate(repeaterID string) (telemetry.Sample, bool) {
	return s.services.LatestErrorRate(repeaterID)
}

func permitsCircuit(service *repair.PermitService, stationID, circuitID string) (*feed.Circuit, bool) {
	return service.Circuit(stationID, circuitID)
}
