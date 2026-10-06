package capacity

import (
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/topology"
)

type fakeTopology struct {
	snapshot topology.Snapshot
}

func (f *fakeTopology) Snapshot(string) (topology.Snapshot, error) { return f.snapshot, nil }

func newTestService(t *testing.T, now time.Time, generation uint64, active topology.Path, in Inventory) (*Service, *fakeTopology) {
	t.Helper()
	estimator, err := NewEstimator(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	store := NewInventoryStore()
	if err := store.Put(in); err != nil {
		t.Fatal(err)
	}
	topo := &fakeTopology{snapshot: topology.Snapshot{
		SpanID: "S3", Generation: generation, ActivePath: active, ChangedAt: now,
	}}
	service, err := NewService(estimator, topo, store, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	return service, topo
}

func TestInventoryStoreRejectsOlderGeneration(t *testing.T) {
	now := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	store := NewInventoryStore()
	in := fullInventory(now, 5, PathSpec{}, PathSpec{})
	if err := store.Put(in); err != nil {
		t.Fatal(err)
	}
	older := fullInventory(now, 4, PathSpec{}, PathSpec{})
	if err := store.Put(older); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("older generation must not overwrite, got %v", err)
	}
	newer := fullInventory(now, 6, PathSpec{}, PathSpec{})
	if err := store.Put(newer); err != nil {
		t.Fatalf("newer generation must be accepted, got %v", err)
	}
	got, _ := store.Get("S3")
	if got.Generation != 6 {
		t.Fatalf("store held generation %d, want 6", got.Generation)
	}
}

func TestAssessSwitchSafeWithHealthyStandbyMargin(t *testing.T) {
	now := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	in := fullInventory(now, 1, PathSpec{}, PathSpec{})
	service, _ := newTestService(t, now, 1, topology.PathPrimary, in)

	assessment, err := service.AssessSwitch("S3", 100_000)
	if err != nil {
		t.Fatalf("fully evidenced switch must not error: %v", err)
	}
	if assessment.Blocked || assessment.Level != RiskSafe {
		t.Fatalf("expected safe switch, got %+v", assessment)
	}
	if assessment.StandbyPath != topology.PathProtection || assessment.StandbyHeadroomBPS <= 0 {
		t.Fatalf("standby margin not compared: %+v", assessment)
	}
	if assessment.Generation != 1 || assessment.LiveGeneration != 1 || !assessment.AsOf.Equal(now.Add(-time.Minute)) {
		t.Fatalf("assessment must carry generation and telemetry binding: %+v", assessment)
	}
}

func TestAssessSwitchBlocksWhenStandbyBERAtOutageFloor(t *testing.T) {
	now := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	primary := healthySpec(topology.PathPrimary, 100, 1e-12, now.Add(-time.Minute), 1)
	protection := healthySpec(topology.PathProtection, 100, 1e-3, now.Add(-time.Minute), 1)
	service, _ := newTestService(t, now, 1, topology.PathPrimary,
		fullInventory(now, 1, primary, protection))

	assessment, err := service.AssessSwitch("S3", 100_000)
	if err != nil {
		t.Fatalf("a present BER at the floor is complete evidence, not a data gap: %v", err)
	}
	if !assessment.Blocked || assessment.Level != RiskBlocked || assessment.StandbyCapacityBPS != 0 {
		t.Fatalf("zero-capacity standby must block the switch: %+v", assessment)
	}
}

func TestAssessSwitchBlocksOnZeroCapacityStandby(t *testing.T) {
	now := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	primary := healthySpec(topology.PathPrimary, 100, 1e-12, now.Add(-time.Minute), 1)
	protection := healthySpec(topology.PathProtection, 100, 1e-12, now.Add(-time.Minute), 1)
	protection.Repeaters = append(protection.Repeaters, RepeaterState{ID: "rx-dead", Health: repeaterTripped})
	service, _ := newTestService(t, now, 1, topology.PathPrimary,
		fullInventory(now, 1, primary, protection))

	assessment, err := service.AssessSwitch("S3", 100_000)
	if err != nil {
		t.Fatalf("complete evidence must not error even when blocking: %v", err)
	}
	if !assessment.Blocked || assessment.Level != RiskBlocked {
		t.Fatalf("tripped standby repeater must block, got %+v", assessment)
	}
}

func TestAssessSwitchWarnsOnThinMargin(t *testing.T) {
	now := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	in := fullInventory(now, 1, PathSpec{}, PathSpec{})
	service, _ := newTestService(t, now, 1, topology.PathPrimary, in)

	// Healthy path bears ~215k bps at 100 km; 200k leaves < 15% headroom.
	assessment, err := service.AssessSwitch("S3", 200_000)
	if err != nil {
		t.Fatal(err)
	}
	if assessment.Blocked || assessment.Level != RiskCaution {
		t.Fatalf("thin margin must be a caution, got %+v", assessment)
	}
}

func TestAssessSwitchBlockedByGenerationDrift(t *testing.T) {
	now := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	in := fullInventory(now, 1, PathSpec{}, PathSpec{})
	service, topo := newTestService(t, now, 1, topology.PathPrimary, in)

	// The cross-connect switches after the inventory was bound.
	topo.snapshot.Generation = 2
	topo.snapshot.ActivePath = topology.PathProtection

	assessment, err := service.AssessSwitch("S3", 100_000)
	if !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("generation drift must be reported, got %v", err)
	}
	if !assessment.Blocked || assessment.Generation != 1 || assessment.LiveGeneration != 2 {
		t.Fatalf("stale-generation evidence must block the switch: %+v", assessment)
	}
}

func TestAssessSwitchBlockedByStaleTelemetry(t *testing.T) {
	now := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	primary := healthySpec(topology.PathPrimary, 100, 1e-12, now.Add(-time.Hour), 1)
	protection := healthySpec(topology.PathProtection, 100, 1e-12, now.Add(-time.Hour), 1)
	service, _ := newTestService(t, now, 1, topology.PathPrimary,
		fullInventory(now, 1, primary, protection))

	assessment, err := service.AssessSwitch("S3", 100_000)
	if !errors.Is(err, ErrStaleTelemetry) || !assessment.Blocked {
		t.Fatalf("stale telemetry must block the switch, got err=%v assessment=%+v", err, assessment)
	}
}

func TestRecommendExpansionApprovedWhenEvidenceComplete(t *testing.T) {
	now := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	in := fullInventory(now, 3, PathSpec{}, PathSpec{})
	service, _ := newTestService(t, now, 3, topology.PathPrimary, in)

	advice, err := service.RecommendExpansion("S3", 100_000)
	if err != nil || !advice.Approved || len(advice.Candidates) != 2 {
		t.Fatalf("complete evidence should approve expansion, got err=%v advice=%+v", err, advice)
	}
	if advice.Generation != 3 || advice.LiveGeneration != 3 {
		t.Fatalf("advice must bind generations: %+v", advice)
	}
}

func TestRecommendExpansionRefusedWhenDataMissing(t *testing.T) {
	now := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	primary := healthySpec(topology.PathPrimary, 100, 0, time.Time{}, 1)
	in := fullInventory(now, 1, primary, PathSpec{})
	service, _ := newTestService(t, now, 1, topology.PathPrimary, in)

	advice, err := service.RecommendExpansion("S3", 100_000)
	if !errors.Is(err, ErrInsufficientData) || advice.Approved {
		t.Fatalf("missing telemetry must refuse automatic expansion, got err=%v advice=%+v", err, advice)
	}
	if len(advice.Reasons) == 0 {
		t.Fatal("refusal must explain the data gap")
	}
}

func TestRecommendExpansionRefusedAcrossGenerationDrift(t *testing.T) {
	now := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	in := fullInventory(now, 1, PathSpec{}, PathSpec{})
	service, topo := newTestService(t, now, 1, topology.PathPrimary, in)
	topo.snapshot.Generation = 2

	advice, err := service.RecommendExpansion("S3", 100_000)
	if !errors.Is(err, ErrGenerationMismatch) || advice.Approved {
		t.Fatalf("generation drift must refuse expansion, got err=%v advice=%+v", err, advice)
	}
}

func TestRecommendExpansionDeniedWhenNeitherPathFits(t *testing.T) {
	now := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	in := fullInventory(now, 1, PathSpec{}, PathSpec{})
	service, _ := newTestService(t, now, 1, topology.PathPrimary, in)

	// Paths bear ~215k bps at 100 km; 500k fits on neither.
	advice, err := service.RecommendExpansion("S3", 500_000)
	if err != nil || advice.Approved || len(advice.Candidates) != 0 {
		t.Fatalf("oversized request must be denied without error, got err=%v advice=%+v", err, advice)
	}
}
