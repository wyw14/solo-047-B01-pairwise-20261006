package capacity

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/wyw14/cry-103/internal/topology"
)

// Repeater health values, mirroring internal/repeater.HealthState. They are
// duplicated as literals so this package does not depend on the projector.
const (
	repeaterHealthy = "healthy"
	repeaterHot     = "hot"
	repeaterTripped = "latched-trip"
)

const (
	// minLengthFactor bounds how hard attenuation alone may penalise a path.
	minLengthFactor = 0.05
	// minBERFactor is the residual spectral-efficiency factor at the BER
	// floor, just before the hard outage.
	minBERFactor = 0.1
	// conservativeFactorWhenBERMissing is applied when BER telemetry is
	// absent. It is deliberately pessimistic: an unmeasured path must never
	// look healthier than a measured one.
	conservativeFactorWhenBERMissing = 0.5
	// hotRepeaterFactor penalises each over-temperature repeater that has not
	// yet latched off.
	hotRepeaterFactor = 0.8
)

// Estimator turns an Inventory into bound capacity figures. It is safe for
// concurrent use because it holds only immutable configuration.
type Estimator struct {
	cfg Config
}

// NewEstimator validates cfg and returns an estimator.
func NewEstimator(cfg Config) (*Estimator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Estimator{cfg: cfg}, nil
}

// LengthFactor models attenuation: loss grows linearly with distance and the
// resulting OSNR penalty maps to capacity through a 10*gamma dB power rule.
func (e *Estimator) LengthFactor(lengthKM float64) float64 {
	if lengthKM <= 0 {
		return 0
	}
	lossDB := lengthKM * e.cfg.LengthAttenuationDBPerKM
	factor := math.Pow(10, -lossDB/(10.0*3.0))
	if factor < minLengthFactor {
		return minLengthFactor
	}
	return factor
}

// BERFactor maps bit error rate to a spectral-efficiency factor. BER at or
// above the floor is a hard outage; BER at/below target is full rate; values
// in between are interpolated logarithmically.
func (e *Estimator) BERFactor(ber float64) float64 {
	if ber >= e.cfg.BERFloor {
		return 0
	}
	if ber <= e.cfg.BERTarget {
		return 1
	}
	position := (math.Log10(ber) - math.Log10(e.cfg.BERTarget)) /
		(math.Log10(e.cfg.BERFloor) - math.Log10(e.cfg.BERTarget))
	position = math.Min(1, math.Max(0, position))
	return 1 - position*(1-minBERFactor)
}

// repeaterFactor reduces capacity for degraded repeaters and zeroes it for
// any latched-trip unit, which breaks the optical chain.
func repeaterFactor(repeaters []RepeaterState) (float64, []string) {
	hot := 0
	var tripped []string
	for _, unit := range repeaters {
		switch unit.Health {
		case repeaterTripped:
			tripped = append(tripped, unit.ID)
		case repeaterHot:
			hot++
		}
	}
	if len(tripped) > 0 {
		return 0, tripped
	}
	return math.Pow(hotRepeaterFactor, float64(hot)), nil
}

// EstimatePath computes one path budget bound to boundGeneration (the
// topology generation of the enclosing inventory), recording every input it
// could not evidence. The returned budget is usable even with gaps, but
// callers must treat HasDataGap as vetoing automated advice.
func (e *Estimator) EstimatePath(spec PathSpec, boundGeneration uint64, now time.Time) PathBudget {
	budget := PathBudget{
		Path:     spec.Path,
		LengthKM: spec.LengthKM,
		BER:      spec.BER,
	}
	if spec.Path != topology.PathPrimary && spec.Path != topology.PathProtection {
		budget.DataGaps = append(budget.DataGaps, "path identity is invalid")
	}
	budget.LengthFactor = e.LengthFactor(spec.LengthKM)
	if spec.LengthKM <= 0 {
		budget.DataGaps = append(budget.DataGaps, "fibre segment length is missing")
	}
	if len(spec.Repeaters) == 0 {
		budget.DataGaps = append(budget.DataGaps, "no repeaters registered on path")
		budget.RepeaterFactor = 1
	} else {
		factor, tripped := repeaterFactor(spec.Repeaters)
		budget.RepeaterFactor = factor
		budget.TrippedRepeaters = tripped
	}
	switch {
	case spec.BER <= 0 || spec.BERObservedAt.IsZero():
		budget.DataGaps = append(budget.DataGaps, "bit error rate telemetry is missing")
		budget.BERFactor = conservativeFactorWhenBERMissing
	case spec.BERGeneration != 0 && spec.BERGeneration != boundGeneration:
		budget.DataGaps = append(budget.DataGaps, fmt.Sprintf(
			"bit error rate telemetry belongs to topology generation %d, expected %d",
			spec.BERGeneration, boundGeneration))
		budget.BERFactor = conservativeFactorWhenBERMissing
	case e.age(spec.BERObservedAt, now) > e.cfg.MaxTelemetryAge:
		budget.DataGaps = append(budget.DataGaps, fmt.Sprintf(
			"bit error rate telemetry is stale (%s old)", e.age(spec.BERObservedAt, now).Round(time.Second)))
		budget.BERFactor = e.BERFactor(spec.BER)
	default:
		budget.BERFactor = e.BERFactor(spec.BER)
	}
	budget.EstimatedBPS = int64(math.Floor(
		float64(e.cfg.BaseBPS) * budget.LengthFactor * budget.BERFactor * budget.RepeaterFactor))
	return budget
}

func (e *Estimator) age(t, now time.Time) time.Duration {
	age := now.UTC().Sub(t.UTC())
	if age < 0 {
		return 0
	}
	return age
}

// Report builds both path budgets and binds them to the inventory's topology
// generation and to the newest telemetry timestamp. A non-nil error wraps one
// of the sentinel errors (ErrInsufficientData / ErrStaleTelemetry) but the
// populated report is still returned so callers may display why advice is
// blocked.
func (e *Estimator) Report(in Inventory, active topology.Path, trafficBPS int64, now time.Time) (BudgetReport, error) {
	if err := in.validate(); err != nil {
		return BudgetReport{}, err
	}
	now = now.UTC()
	report := BudgetReport{
		SpanID:     in.SpanID,
		Generation: in.Generation,
		AsOf:       boundTime(in, now),
		Primary:    e.EstimatePath(in.Primary, in.Generation, now),
		Protection: e.EstimatePath(in.Protection, in.Generation, now),
		ActivePath: active,
		TrafficBPS: trafficBPS,
	}
	if active != topology.PathPrimary && active != topology.PathProtection {
		return report, fmt.Errorf("%w: active path %q is invalid", ErrInsufficientData, active)
	}
	if active == topology.PathPrimary {
		report.StandbyPath = topology.PathProtection
		report.Active, report.Standby = report.Primary, report.Protection
	} else {
		report.StandbyPath = topology.PathPrimary
		report.Active, report.Standby = report.Protection, report.Primary
	}
	var problems []error
	for _, budget := range []PathBudget{report.Primary, report.Protection} {
		for _, gap := range budget.DataGaps {
			report.Insufficient = true
			report.Reasons = append(report.Reasons, fmt.Sprintf("%s path: %s", budget.Path, gap))
			switch {
			case strings.Contains(gap, "stale"):
				problems = append(problems, ErrStaleTelemetry)
			case strings.Contains(gap, "generation"):
				problems = append(problems, ErrGenerationMismatch)
			default:
				problems = append(problems, ErrInsufficientData)
			}
		}
	}
	return report, errors.Join(problems...)
}

func (in Inventory) validate() error {
	if in.SpanID == "" || in.Generation == 0 {
		return fmt.Errorf("%w: span identity and topology generation are required", ErrInsufficientData)
	}
	if in.Primary.Path != topology.PathPrimary {
		return fmt.Errorf("%w: primary path inventory is not bound", ErrInsufficientData)
	}
	if in.Protection.Path != topology.PathProtection {
		return fmt.Errorf("%w: protection path inventory is not bound", ErrInsufficientData)
	}
	return nil
}

// boundTime returns the newest BER observation across both paths. When neither
// path has telemetry the estimate is bound to evaluation time but flagged
// insufficient.
func boundTime(in Inventory, fallback time.Time) time.Time {
	bound := time.Time{}
	for _, observed := range []time.Time{in.Primary.BERObservedAt, in.Protection.BERObservedAt} {
		if observed.After(bound) {
			bound = observed.UTC()
		}
	}
	if bound.IsZero() {
		return fallback
	}
	return bound
}
