package capacity

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/topology"
)

func testConfig() Config {
	return Config{
		BaseBPS:                  1_000_000,
		LengthAttenuationDBPerKM: 0.2,
		BERFloor:                 1e-3,
		BERTarget:                1e-12,
		MaxTelemetryAge:          10 * time.Minute,
	}
}

func healthySpec(path topology.Path, lengthKM, ber float64, observedAt time.Time, generation uint64) PathSpec {
	return PathSpec{
		Path:          path,
		LengthKM:      lengthKM,
		Repeaters:     []RepeaterState{{ID: string(path) + "-r1", Health: repeaterHealthy}},
		BER:           ber,
		BERObservedAt: observedAt,
		BERGeneration: generation,
	}
}

func fullInventory(now time.Time, generation uint64, primary, protection PathSpec) Inventory {
	if primary.Path == "" {
		primary = healthySpec(topology.PathPrimary, 100, 1e-12, now.Add(-time.Minute), generation)
	}
	if protection.Path == "" {
		protection = healthySpec(topology.PathProtection, 100, 1e-12, now.Add(-2*time.Minute), generation)
	}
	return Inventory{SpanID: "S3", Generation: generation, Primary: primary, Protection: protection}
}

func approx(t *testing.T, got, want, tolerance float64, label string) {
	t.Helper()
	if math.Abs(got-want) > tolerance {
		t.Fatalf("%s = %.4f, want %.4f (+-%.4f)", label, got, want, tolerance)
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := testConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	cfg.MaxTelemetryAge = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("zero telemetry window must be rejected")
	}
	cfg = testConfig()
	cfg.BERTarget = cfg.BERFloor
	if err := cfg.Validate(); err == nil {
		t.Fatal("BER target must be below the floor")
	}
}

func TestBERFactorBoundaries(t *testing.T) {
	estimator, err := NewEstimator(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if factor := estimator.BERFactor(1e-15); factor != 1 {
		t.Fatalf("BER below target must keep full rate, got %.3f", factor)
	}
	if factor := estimator.BERFactor(1e-3); factor != 0 {
		t.Fatalf("BER at the floor must be an outage, got %.3f", factor)
	}
	// 1e-6 sits 2/3 of the way across the 9-decade log span.
	approx(t, estimator.BERFactor(1e-6), 0.4, 0.001, "ber factor")
}

func TestLengthFactorDecaysWithDistance(t *testing.T) {
	estimator, _ := NewEstimator(testConfig())
	short := estimator.LengthFactor(100)
	long := estimator.LengthFactor(300)
	if !(short > long && long > 0) {
		t.Fatalf("length penalty must grow with distance: short %.4f long %.4f", short, long)
	}
	if estimator.LengthFactor(0) != 0 {
		t.Fatal("zero length has no defined capacity")
	}
	// Very long spans saturate at the attenuation floor rather than zero.
	approx(t, estimator.LengthFactor(10_000), minLengthFactor, 0.0001, "length floor")
}

func TestTrippedRepeaterZeroesCapacity(t *testing.T) {
	now := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	estimator, _ := NewEstimator(testConfig())
	spec := healthySpec(topology.PathProtection, 100, 1e-12, now.Add(-time.Minute), 1)
	spec.Repeaters = append(spec.Repeaters, RepeaterState{ID: "r-dead", Health: repeaterTripped})
	budget := estimator.EstimatePath(spec, 1, now)
	if budget.EstimatedBPS != 0 || budget.RepeaterFactor != 0 {
		t.Fatalf("tripped repeater must break the optical chain: %+v", budget)
	}
	if len(budget.TrippedRepeaters) != 1 || budget.TrippedRepeaters[0] != "r-dead" {
		t.Fatalf("tripped repeater not identified: %v", budget.TrippedRepeaters)
	}
}

func TestMissingAndStaleTelemetryAreDataGaps(t *testing.T) {
	now := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	estimator, _ := NewEstimator(testConfig())

	noBER := healthySpec(topology.PathPrimary, 100, 0, time.Time{}, 1)
	budget := estimator.EstimatePath(noBER, 1, now)
	if !budget.HasDataGap() || budget.BERFactor != conservativeFactorWhenBERMissing {
		t.Fatalf("missing BER must be a conservative data gap: %+v", budget)
	}

	stale := healthySpec(topology.PathPrimary, 100, 1e-6, now.Add(-time.Hour), 1)
	budget = estimator.EstimatePath(stale, 1, now)
	if !budget.HasDataGap() {
		t.Fatalf("stale BER must be flagged: %+v", budget)
	}

	wrongGeneration := healthySpec(topology.PathPrimary, 100, 1e-12, now.Add(-time.Minute), 1)
	budget = estimator.EstimatePath(wrongGeneration, 2, now)
	if !budget.HasDataGap() || budget.BERFactor != conservativeFactorWhenBERMissing {
		t.Fatalf("telemetry from an older topology generation must be a gap: %+v", budget)
	}
}

func TestReportBindsTelemetryTimeAndGeneration(t *testing.T) {
	now := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	estimator, _ := NewEstimator(testConfig())
	primary := healthySpec(topology.PathPrimary, 100, 1e-12, now.Add(-time.Minute), 7)
	protection := healthySpec(topology.PathProtection, 100, 1e-12, now.Add(-2*time.Minute), 7)
	report, err := estimator.Report(
		Inventory{SpanID: "S3", Generation: 7, Primary: primary, Protection: protection},
		topology.PathPrimary, 10_000, now)
	if err != nil {
		t.Fatalf("complete evidence must not error: %v", err)
	}
	if report.Generation != 7 || !report.AsOf.Equal(now.Add(-time.Minute)) {
		t.Fatalf("report must bind the newest telemetry time and inventory generation: %+v", report)
	}
	if report.Active.Path != topology.PathPrimary || report.Standby.Path != topology.PathProtection {
		t.Fatalf("active/standby selection wrong: %+v / %+v", report.Active, report.Standby)
	}
}

func TestReportRefusesIncompleteInventory(t *testing.T) {
	now := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	estimator, _ := NewEstimator(testConfig())

	if _, err := estimator.Report(Inventory{SpanID: "S3"}, topology.PathPrimary, 0, now); !errors.Is(err, ErrInsufficientData) {
		t.Fatalf("unbound inventory must fail with ErrInsufficientData, got %v", err)
	}

	primary := healthySpec(topology.PathPrimary, 100, 0, time.Time{}, 1)
	protection := healthySpec(topology.PathProtection, 100, 1e-12, now.Add(-time.Hour), 1)
	report, err := estimator.Report(
		Inventory{SpanID: "S3", Generation: 1, Primary: primary, Protection: protection},
		topology.PathPrimary, 0, now)
	if !errors.Is(err, ErrInsufficientData) || !errors.Is(err, ErrStaleTelemetry) {
		t.Fatalf("missing and stale telemetry must both be reported, got %v", err)
	}
	if !report.Insufficient || len(report.Reasons) != 2 {
		t.Fatalf("report must explain both gaps, got %+v", report)
	}
}
