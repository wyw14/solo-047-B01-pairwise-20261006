package capacity

import (
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/repeater"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

const (
	baseCapacity = int64(400_000_000_000)
	loadFactor   = 0.8
	maxBER       = 1e-12
)

func testEstimator(t *testing.T) *Estimator {
	t.Helper()
	estimator, err := NewEstimator(Config{
		Staleness:     30 * time.Second,
		LoadFactor:    loadFactor,
		BaseCapacity:  baseCapacity,
		MaxBER:        maxBER,
		HotDerate:     0.7,
		TrippedDerate: 0.0,
	})
	if err != nil {
		t.Fatal(err)
	}
	return estimator
}

func baseInputs(now time.Time) Inputs {
	topo := topology.Snapshot{
		SpanID: "S3", Generation: 7, ActivePath: topology.PathPrimary,
		ChangedAt: now.Add(-time.Minute),
	}
	spanSnapshot := span.Snapshot{ID: "S3", Name: "S3", LengthKM: 120, State: span.StateNormal, UpdatedAt: now}
	routeSnapshot := route.Snapshot{ID: "R-S3", SpanID: "S3", State: route.StatePrimary, ActivePath: topology.PathPrimary, TrafficBPS: 100_000_000_000, UpdatedAt: now}
	return Inputs{
		Span: spanSnapshot, Route: routeSnapshot, Topology: topo, Now: now,
	}
}

func healthyChain(gen uint64, now time.Time, ber float64) (PathSignals, repeater.Snapshot) {
	rp := repeater.Snapshot{ID: "R07", Health: repeater.HealthNormal, UpdatedAt: now}
	sample := telemetry.Sample{
		ID: "s1", AssetID: "R07", Kind: telemetry.KindErrorRate, Value: ber,
		Sequence: 1, Generation: gen, ObservedAt: now.Add(-2 * time.Second),
	}
	return PathSignals{Repeaters: []repeater.Snapshot{rp}, ErrorRates: []telemetry.Sample{sample}}, rp
}

func TestBudgetHealthyPathsSizedAndExpansionAllowed(t *testing.T) {
	now := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	in := baseInputs(now)
	primary, _ := healthyChain(7, now, 0.0)
	protection, _ := healthyChain(7, now, 0.0)
	protection.Repeaters[0].ID = "R08"
	protection.ErrorRates[0].ID = "s2"
	protection.ErrorRates[0].AssetID = "R08"
	in.Primary = primary
	in.Protection = protection

	budget, err := testEstimator(t).Evaluate(in)
	if err != nil {
		t.Fatal(err)
	}
	if !budget.Primary.DataSufficient || !budget.Protection.DataSufficient {
		t.Fatalf("both paths should be sized: %+v / %+v", budget.Primary.Missing, budget.Protection.Missing)
	}
	// 400G * length(1.0) * BER(1.0) * health(1.0) * 0.8 = 320G usable.
	want := int64(320_000_000_000)
	if budget.Primary.EstimatedBPS != want || budget.Protection.EstimatedBPS != want {
		t.Fatalf("estimated bps primary=%d protection=%d want %d", budget.Primary.EstimatedBPS, budget.Protection.EstimatedBPS, want)
	}
	if budget.Primary.HeadroomBPS != want-100_000_000_000 {
		t.Fatalf("unexpected headroom %d", budget.Primary.HeadroomBPS)
	}
	if budget.SwitchRisk != RiskNone {
		t.Fatalf("risk=%s reasons=%v", budget.SwitchRisk, budget.RiskReasons)
	}
	if !budget.Expansion.Allowed || budget.Expansion.ExpandableBPS != want-100_000_000_000 {
		t.Fatalf("expansion should be allowed: %+v", budget.Expansion)
	}
	if budget.ActiveGeneration != 7 || !budget.TelemetryBoundAt.Equal(now.Add(-2*time.Second)) {
		t.Fatalf("budget not bound correctly: gen=%d bound=%s", budget.ActiveGeneration, budget.TelemetryBoundAt)
	}
	if budget.TopologyMismatch || budget.Stale {
		t.Fatalf("budget unexpectedly flagged mismatch/stale")
	}
}

func TestBudgetInsufficientDataRefusesExpansionAndBlocksSwitch(t *testing.T) {
	now := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	in := baseInputs(now)
	primary, _ := healthyChain(7, now, 0.0)
	in.Primary = primary
	// Protection: repeaters exist but no BER telemetry at all.
	in.Protection = PathSignals{Repeaters: []repeater.Snapshot{{ID: "R08", Health: repeater.HealthNormal, UpdatedAt: now}}}

	budget, err := testEstimator(t).Evaluate(in)
	if err != nil {
		t.Fatal(err)
	}
	if budget.Protection.DataSufficient {
		t.Fatal("protection must not be sized without BER")
	}
	if budget.Expansion.Allowed {
		t.Fatalf("auto-expansion must be refused on insufficient data: %+v", budget.Expansion)
	}
	if budget.SwitchRisk != RiskUncertain {
		t.Fatalf("risk=%s, want uncertain", budget.SwitchRisk)
	}

	_, guardErr := NewGuard(testEstimator(t)).Check(in)
	if guardErr == nil {
		t.Fatal("guard must reject an unsized protection path")
	}
	var ge *GuardError
	if !errors.As(guardErr, &ge) || ge.Category != RiskCategoryBlocked {
		t.Fatalf("uncertain risk must be a hard (non-forceable) denial: %v", guardErr)
	}
}

func TestBudgetProtectionOvercommittedIsBlockedAndNotForceable(t *testing.T) {
	now := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	in := baseInputs(now)
	in.Route.TrafficBPS = 350_000_000_000 // exceeds 320G usable
	primary, _ := healthyChain(7, now, 0.0)
	protection, _ := healthyChain(7, now, 0.0)
	protection.Repeaters[0].ID = "R08"
	protection.ErrorRates[0].AssetID = "R08"
	in.Primary = primary
	in.Protection = protection

	budget, err := testEstimator(t).Evaluate(in)
	if err != nil {
		t.Fatal(err)
	}
	if !budget.Protection.Overcommitted {
		t.Fatal("protection should be overcommitted")
	}
	if budget.SwitchRisk != RiskBlocked {
		t.Fatalf("risk=%s want blocked", budget.SwitchRisk)
	}
	if budget.Expansion.Allowed {
		t.Fatal("expansion refused while overcommitted")
	}

	service := NewService(&stubBuilder{inputs: in}, testEstimator(t))
	if _, err := service.Force("R-S3"); err == nil {
		t.Fatal("force must not override a blocked protection path")
	}
}

func TestBudgetSoftRiskIsForceable(t *testing.T) {
	now := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	in := baseInputs(now)
	// Load chosen so protection margin is under 10%: usable 320G, load 300G.
	in.Route.TrafficBPS = 300_000_000_000
	primary, _ := healthyChain(7, now, 0.0)
	protection, _ := healthyChain(7, now, 0.0)
	protection.Repeaters[0].ID = "R08"
	protection.ErrorRates[0].AssetID = "R08"
	in.Primary = primary
	in.Protection = protection

	service := NewService(&stubBuilder{inputs: in}, testEstimator(t))
	_, err := service.PreSwitchCheck("R-S3")
	var ge *GuardError
	if !errors.As(err, &ge) || ge.Category != RiskCategorySoft {
		t.Fatalf("want soft guard error, got %v", err)
	}
	forced, err := service.Force("R-S3")
	if err != nil {
		t.Fatalf("force should override soft risk: %v", err)
	}
	if forced.SwitchRisk != RiskElevated {
		t.Fatalf("forced budget risk=%s want elevated", forced.SwitchRisk)
	}
}

func TestBudgetStaleTelemetryRefusesExpansion(t *testing.T) {
	now := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	in := baseInputs(now)
	primary, _ := healthyChain(7, now.Add(-2*time.Minute), 0.0)
	protection, _ := healthyChain(7, now.Add(-2*time.Minute), 0.0)
	protection.Repeaters[0].ID = "R08"
	protection.ErrorRates[0].AssetID = "R08"
	in.Primary = primary
	in.Protection = protection

	budget, err := testEstimator(t).Evaluate(in)
	if err != nil {
		t.Fatal(err)
	}
	if !budget.Stale {
		t.Fatal("budget should be marked stale")
	}
	if budget.Expansion.Allowed {
		t.Fatalf("stale telemetry must refuse expansion: %+v", budget.Expansion)
	}
}

func TestBudgetGenerationMismatchIsUncertainAndRefusesEverything(t *testing.T) {
	now := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	in := baseInputs(now)                   // topology generation 7
	primary, _ := healthyChain(6, now, 0.0) // stale generation
	protection, _ := healthyChain(7, now, 0.0)
	protection.Repeaters[0].ID = "R08"
	protection.ErrorRates[0].AssetID = "R08"
	in.Primary = primary
	in.Protection = protection

	budget, err := testEstimator(t).Evaluate(in)
	if err != nil {
		t.Fatal(err)
	}
	if !budget.TopologyMismatch {
		t.Fatal("generation mismatch must be flagged")
	}
	if budget.Primary.DataSufficient {
		t.Fatal("primary has only a stale-generation sample and must be insufficient")
	}
	if budget.SwitchRisk != RiskUncertain {
		t.Fatalf("risk=%s want uncertain", budget.SwitchRisk)
	}
	if budget.Expansion.Allowed {
		t.Fatal("expansion must be refused on generation mismatch")
	}

	service := NewService(&stubBuilder{inputs: in}, testEstimator(t))
	if _, err := service.Force("R-S3"); err == nil {
		t.Fatal("force must not override uncertain evidence")
	}
}

func TestBudgetBERCeilingAndTrippedRepeaterZeroCapacity(t *testing.T) {
	now := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	in := baseInputs(now)
	primary, _ := healthyChain(7, now, 0.0)
	// Protection BER over ceiling.
	protectionSignals, _ := healthyChain(7, now, 5e-12)
	protectionSignals.Repeaters[0].ID = "R08"
	protectionSignals.ErrorRates[0].AssetID = "R08"
	in.Primary = primary
	in.Protection = protectionSignals

	budget, err := testEstimator(t).Evaluate(in)
	if err != nil {
		t.Fatal(err)
	}
	if budget.Protection.EstimatedBPS != 0 || budget.Protection.BERFactor != 0 {
		t.Fatalf("over-ceiling BER must zero capacity: %+v", budget.Protection)
	}
	if budget.SwitchRisk != RiskBlocked {
		t.Fatalf("risk=%s want blocked", budget.SwitchRisk)
	}

	// Tripped repeater on an otherwise clean path also zeroes capacity.
	in2 := baseInputs(now)
	in2.Primary = primary
	tripped := []repeater.Snapshot{{ID: "R08", Health: repeater.HealthTripped, UpdatedAt: now}}
	in2.Protection = PathSignals{Repeaters: tripped, ErrorRates: protectionSignals.ErrorRates}
	budget2, err := testEstimator(t).Evaluate(in2)
	if err != nil {
		t.Fatal(err)
	}
	if budget2.Protection.HealthFactor != 0 || budget2.Protection.EstimatedBPS != 0 {
		t.Fatalf("tripped repeater must zero capacity: factor=%f bps=%d", budget2.Protection.HealthFactor, budget2.Protection.EstimatedBPS)
	}
}

func TestLengthPenaltyDeratesLongSpan(t *testing.T) {
	if got := lengthPenalty(120); got != 1.0 {
		t.Fatalf("reference length factor=%f want 1.0", got)
	}
	// 320 km: 1 - 0.0025*200 = 0.5
	if got := lengthPenalty(320); got != 0.5 {
		t.Fatalf("long span factor=%f want 0.5", got)
	}
	// 1000 km floors at 0.25.
	if got := lengthPenalty(1000); got != 0.25 {
		t.Fatalf("floor factor=%f want 0.25", got)
	}
}

func TestInvalidConfigRejected(t *testing.T) {
	cases := []Config{
		{Staleness: 0, LoadFactor: 0.8, BaseCapacity: 1, MaxBER: 1, HotDerate: 0.7, TrippedDerate: 0},
		{Staleness: time.Second, LoadFactor: 1, BaseCapacity: 1, MaxBER: 1, HotDerate: 0.7, TrippedDerate: 0},
		{Staleness: time.Second, LoadFactor: 0.8, BaseCapacity: 1, MaxBER: 1, HotDerate: 0.5, TrippedDerate: 0.6},
	}
	for i, cfg := range cases {
		if _, err := NewEstimator(cfg); !errors.Is(err, ErrInvalidTuning) {
			t.Fatalf("case %d: want ErrInvalidTuning got %v", i, err)
		}
	}
}

func TestInputsMustReferenceSameSpan(t *testing.T) {
	now := time.Now()
	in := baseInputs(now)
	in.Route.SpanID = "OTHER"
	if _, err := testEstimator(t).Evaluate(in); err == nil {
		t.Fatal("mismatched span inputs must be rejected")
	}
}

// stubBuilder returns fixed inputs without needing a SnapshotSource.
type stubBuilder struct {
	inputs Inputs
}

func (b *stubBuilder) Build(_ string) (Inputs, error) { return b.inputs, nil }
