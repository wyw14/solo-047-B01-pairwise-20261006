package capacity

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/wyw14/cry-103/internal/repeater"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

// RiskLevel describes how dangerous it would be to move traffic onto a path.
type RiskLevel string

const (
	RiskNone      RiskLevel = "none"
	RiskLow       RiskLevel = "low"
	RiskElevated  RiskLevel = "elevated"
	RiskHigh      RiskLevel = "high"
	RiskBlocked   RiskLevel = "blocked"
	RiskUncertain RiskLevel = "uncertain"
)

// Path mirrors topology.Path so callers can pass either interchangeably.
type Path = topology.Path

// PathSignals is the live evidence for one candidate path: the repeater chain
// currently provisioned on it and the latest error-rate sample per repeater.
type PathSignals struct {
	Repeaters  []repeater.Snapshot
	ErrorRates []telemetry.Sample
}

// Inputs collects the live evidence a budget is derived from. Telemetry is
// accepted only when it is fresh relative to Now and carries the same
// generation as the topology snapshot.
type Inputs struct {
	Span       span.Snapshot
	Route      route.Snapshot
	Topology   topology.Snapshot
	Primary    PathSignals
	Protection PathSignals
	Now        time.Time

	// Optional per-evaluation overrides; zero means use the estimator default.
	Staleness     time.Duration
	LoadFactor    float64
	BaseCapacity  int64
	MaxBER        float64
	HotDerate     float64
	TrippedDerate float64
}

// PathBudget is the estimated headroom on a single candidate path.
type PathBudget struct {
	Path           Path      `json:"path"`
	EstimatedBPS   int64     `json:"estimated_bps"`
	CurrentLoadBPS int64     `json:"current_load_bps"`
	HeadroomBPS    int64     `json:"headroom_bps"`
	Utilization    float64   `json:"utilization"`
	LengthFactor   float64   `json:"length_factor"`
	BERFactor      float64   `json:"ber_factor"`
	HealthFactor   float64   `json:"health_factor"`
	RepeaterCount  int       `json:"repeater_count"`
	Overcommitted  bool      `json:"overcommitted"`
	Stale          bool      `json:"stale"`
	DataSufficient bool      `json:"data_sufficient"`
	Missing        []string  `json:"missing_signals"`
	LatestSampleAt time.Time `json:"latest_sample_at"`
	OldestSampleAt time.Time `json:"oldest_sample_at"`
	Generation     uint64    `json:"generation"`
}

// Recommendation is the verdict on whether the line may carry more traffic.
type Recommendation struct {
	Allowed       bool   `json:"allowed"`
	Reason        string `json:"reason"`
	ExpandableBPS int64  `json:"expandable_bps"`
}

// Budget binds an estimate to the exact telemetry time and topology generation
// it was computed from. It must never be acted upon once either moves.
type Budget struct {
	SpanID           string         `json:"span_id"`
	CurrentPath      Path           `json:"current_path"`
	ActiveGeneration uint64         `json:"active_generation"`
	TelemetryBoundAt time.Time      `json:"telemetry_bound_at"`
	ComputedAt       time.Time      `json:"computed_at"`
	Primary          PathBudget     `json:"primary"`
	Protection       PathBudget     `json:"protection"`
	SwitchRisk       RiskLevel      `json:"switch_risk"`
	RiskReasons      []string       `json:"risk_reasons"`
	Expansion        Recommendation `json:"expansion"`
	Stale            bool           `json:"stale"`
	TopologyMismatch bool           `json:"topology_generation_mismatch"`
}

// Errors returned before or alongside a budget.
var (
	ErrGenerationMismatch = errors.New("capacity: telemetry generation does not match topology generation")
	ErrInsufficientData   = errors.New("capacity: insufficient data to size capacity")
	ErrInvalidTuning      = errors.New("capacity: invalid tuning parameters")
)

// Estimator turns live snapshots plus telemetry into a generation-bound Budget.
type Estimator struct {
	staleness     time.Duration
	loadFactor    float64
	baseCapacity  int64
	maxBER        float64
	hotDerate     float64
	trippedDerate float64
}

// Config configures an Estimator.
type Config struct {
	Staleness     time.Duration // maximum acceptable telemetry age
	LoadFactor    float64       // design utilisation target, e.g. 0.8
	BaseCapacity  int64         // design capacity of a lossless span, bps
	MaxBER        float64       // service BER ceiling, e.g. 1e-12
	HotDerate     float64       // capacity factor for a hot repeater
	TrippedDerate float64       // capacity factor for a latched-trip repeater
}

// NewEstimator validates tuning and returns an estimator.
func NewEstimator(cfg Config) (*Estimator, error) {
	if cfg.Staleness <= 0 || cfg.LoadFactor <= 0 || cfg.LoadFactor >= 1 ||
		cfg.BaseCapacity <= 0 || cfg.MaxBER <= 0 ||
		cfg.HotDerate <= 0 || cfg.HotDerate > 1 ||
		cfg.TrippedDerate < 0 || cfg.TrippedDerate >= cfg.HotDerate {
		return nil, ErrInvalidTuning
	}
	return &Estimator{
		staleness:     cfg.Staleness,
		loadFactor:    cfg.LoadFactor,
		baseCapacity:  cfg.BaseCapacity,
		maxBER:        cfg.MaxBER,
		hotDerate:     cfg.HotDerate,
		trippedDerate: cfg.TrippedDerate,
	}, nil
}

func (e *Estimator) Staleness() time.Duration { return e.staleness }

// Evaluate produces a budget for the span. Both paths are always sized so the
// caller can compare headroom; a path that cannot be sized carries a
// missing-signal list rather than a number.
func (e *Estimator) Evaluate(in Inputs) (Budget, error) {
	if err := in.Topology.Validate(); err != nil {
		return Budget{}, fmt.Errorf("capacity: %w", err)
	}
	if in.Span.ID == "" || in.Route.ID == "" || in.Now.IsZero() {
		return Budget{}, fmt.Errorf("capacity: span, route and evaluation time are required")
	}
	if in.Topology.SpanID != in.Span.ID || in.Route.SpanID != in.Span.ID {
		return Budget{}, fmt.Errorf("capacity: inputs do not all refer to span %s", in.Span.ID)
	}

	staleness, loadFactor, base := e.staleness, e.loadFactor, e.baseCapacity
	maxBER, hot, tripped := e.maxBER, e.hotDerate, e.trippedDerate
	if in.Staleness != 0 {
		staleness = in.Staleness
	}
	if in.LoadFactor != 0 {
		if in.LoadFactor <= 0 || in.LoadFactor >= 1 {
			return Budget{}, ErrInvalidTuning
		}
		loadFactor = in.LoadFactor
	}
	if in.BaseCapacity != 0 {
		base = in.BaseCapacity
	}
	if in.MaxBER != 0 {
		maxBER = in.MaxBER
	}
	if in.HotDerate != 0 {
		hot = in.HotDerate
	}
	if in.TrippedDerate != 0 {
		tripped = in.TrippedDerate
	}
	if staleness <= 0 || loadFactor <= 0 || loadFactor >= 1 || base <= 0 || maxBER <= 0 {
		return Budget{}, ErrInvalidTuning
	}

	generation := in.Topology.Generation
	tuning := pathTuning{staleness, loadFactor, base, maxBER, hot, tripped}
	primary := e.evaluatePath(topology.PathPrimary, in.Primary, in, generation, tuning)
	protection := e.evaluatePath(topology.PathProtection, in.Protection, in, generation, tuning)

	budget := Budget{
		SpanID:           in.Span.ID,
		CurrentPath:      in.Route.ActivePath,
		ActiveGeneration: generation,
		TelemetryBoundAt: later(primary.LatestSampleAt, protection.LatestSampleAt),
		ComputedAt:       in.Now.UTC(),
		Primary:          primary,
		Protection:       protection,
		Stale:            primary.Stale || protection.Stale,
	}

	// Bind telemetry to the topology generation: a BER sample or a repeater
	// trip from a different generation predates a cross-connect change and
	// makes the evidence unsafe to act on.
	budget.TopologyMismatch = generationMismatch(generation, in.Primary) || generationMismatch(generation, in.Protection)
	if budget.TopologyMismatch {
		budget.RiskReasons = append(budget.RiskReasons, "telemetry/topology generation mismatch: evidence is not bound to topology generation "+fmt.Sprint(generation))
	}

	assessSwitchRisk(&budget)
	assessExpansion(&budget)
	return budget, nil
}

type pathTuning struct {
	staleness  time.Duration
	loadFactor float64
	base       int64
	maxBER     float64
	hot        float64
	tripped    float64
}

// evaluatePath sizes one path from that path's own repeater chain and BER
// telemetry. The load it must carry is the route's current traffic.
func (e *Estimator) evaluatePath(path topology.Path, sig PathSignals, in Inputs, generation uint64, t pathTuning) PathBudget {
	b := PathBudget{
		Path:           path,
		Generation:     generation,
		CurrentLoadBPS: in.Route.TrafficBPS,
		DataSufficient: true,
		LengthFactor:   lengthPenalty(in.Span.LengthKM),
	}

	healthFactor, repeaterCount, healthMissing := healthPenalty(sig.Repeaters, t.hot, t.tripped)
	b.HealthFactor = healthFactor
	b.RepeaterCount = repeaterCount
	b.Missing = append(b.Missing, healthMissing...)

	berFactor, latest, oldest, berMissing, stale, inGeneration := berPenalty(sig.ErrorRates, in.Now, t.staleness, t.maxBER, generation)
	b.BERFactor = berFactor
	b.LatestSampleAt = latest
	b.OldestSampleAt = oldest
	b.Stale = stale
	if stale {
		b.Missing = append(b.Missing, "error-rate telemetry is stale")
	}
	b.Missing = append(b.Missing, berMissing...)

	if repeaterCount == 0 {
		b.Missing = append(b.Missing, "no repeaters provisioned on path")
	}
	if len(sig.ErrorRates) == 0 {
		b.Missing = append(b.Missing, "no error-rate telemetry on path")
	} else if !inGeneration {
		b.Missing = append(b.Missing, "no error-rate sample matches the active topology generation")
	}

	if len(b.Missing) > 0 {
		b.DataSufficient = false
		return b
	}

	usable := float64(t.base) * b.LengthFactor * b.BERFactor * b.HealthFactor * t.loadFactor
	b.EstimatedBPS = int64(math.Floor(usable))
	if b.EstimatedBPS < 0 {
		b.EstimatedBPS = 0
	}
	b.HeadroomBPS = b.EstimatedBPS - b.CurrentLoadBPS
	b.Overcommitted = b.HeadroomBPS < 0
	if b.EstimatedBPS > 0 {
		b.Utilization = float64(b.CurrentLoadBPS) / float64(b.EstimatedBPS)
	} else {
		b.Utilization = math.Inf(1)
	}
	return b
}

// lengthPenalty models excess attenuation on long spans. 120 km is the
// reference length; beyond it usable margin shrinks gradually.
func lengthPenalty(lengthKM float64) float64 {
	if lengthKM <= 0 {
		return 0
	}
	const referenceKM = 120.0
	return clamp(1.0-0.0025*(lengthKM-referenceKM), 0.25, 1.0)
}

// healthPenalty returns the chain factor (the weakest repeater dominates),
// the repeater count and descriptors for repeaters whose state is unknown.
func healthPenalty(repeaters []repeater.Snapshot, hot, tripped float64) (float64, int, []string) {
	if len(repeaters) == 0 {
		return 1, 0, nil
	}
	var missing []string
	factor := 1.0
	for _, rp := range repeaters {
		switch rp.Health {
		case repeater.HealthNormal:
			// no derate
		case repeater.HealthHot:
			factor *= hot
		case repeater.HealthTripped:
			factor *= tripped
		default:
			missing = append(missing, fmt.Sprintf("repeater %s health unknown", rp.ID))
			factor *= hot // fail toward the conservative non-zero derate
		}
		if rp.UpdatedAt.IsZero() {
			missing = append(missing, fmt.Sprintf("repeater %s has no health timestamp", rp.ID))
		}
	}
	return clamp(factor, 0, 1), len(repeaters), missing
}

// berPenalty maps the worst in-generation BER onto a capacity factor and
// tracks sample freshness. Out-of-generation samples are excluded from the
// factor and surfaced as missing evidence.
func berPenalty(samples []telemetry.Sample, now time.Time, staleness time.Duration, maxBER float64, generation uint64) (factor float64, latest, oldest time.Time, missing []string, stale, inGeneration bool) {
	factor = 1
	for _, sample := range samples {
		if sample.Generation != generation {
			missing = append(missing, fmt.Sprintf("sample %s generation %d != topology generation %d", sample.ID, sample.Generation, generation))
			continue
		}
		inGeneration = true
		if latest.IsZero() || sample.ObservedAt.After(latest) {
			latest = sample.ObservedAt
		}
		if oldest.IsZero() || sample.ObservedAt.Before(oldest) {
			oldest = sample.ObservedAt
		}
		if now.Sub(sample.ObservedAt) > staleness {
			stale = true
		}
	}
	worst := worstBER(samples, generation)
	if !inGeneration {
		return 1, latest, oldest, missing, stale, false
	}
	if worst > maxBER {
		// Measured but beyond the service ceiling: this is positive evidence
		// that the path is out of spec, not absent data — factor zero drives a
		// blocked risk rather than an uncertain/insufficient verdict.
		return 0, latest, oldest, missing, stale, true
	}
	// Linear derate from 1.0 at zero BER to 0.25 at the ceiling.
	factor = clamp(1.0-0.75*(worst/maxBER), 0.25, 1.0)
	return factor, latest, oldest, missing, stale, true
}

func worstBER(samples []telemetry.Sample, generation uint64) float64 {
	worst := 0.0
	for _, sample := range samples {
		if sample.Generation == generation && sample.Value > worst {
			worst = sample.Value
		}
	}
	return worst
}

func generationMismatch(generation uint64, sig PathSignals) bool {
	for _, sample := range sig.ErrorRates {
		if sample.Generation != generation {
			return true
		}
	}
	for _, rp := range sig.Repeaters {
		// A trip recorded against a newer generation means the topology
		// snapshot handed to us is behind the plant.
		if rp.TripGeneration > generation {
			return true
		}
	}
	return false
}

// assessSwitchRisk assigns a pre-switch risk from the protection path's
// ability to inherit the current load.
func assessSwitchRisk(b *Budget) {
	primary, protection := b.Primary, b.Protection

	if b.TopologyMismatch {
		b.SwitchRisk = RiskUncertain
		return
	}
	if !protection.DataSufficient {
		b.SwitchRisk = RiskUncertain
		b.RiskReasons = append(b.RiskReasons, "protection path data insufficient: "+joinMissing(protection.Missing))
		return
	}
	if protection.EstimatedBPS == 0 {
		b.SwitchRisk = RiskBlocked
		b.RiskReasons = append(b.RiskReasons, "protection path estimated capacity is zero (BER ceiling or hard fault)")
		return
	}
	if protection.Overcommitted {
		b.SwitchRisk = RiskBlocked
		b.RiskReasons = append(b.RiskReasons, fmt.Sprintf("protection path is short by %d bps at the current load", -protection.HeadroomBPS))
		return
	}

	risk := RiskNone
	if protection.HealthFactor <= 0.6 {
		risk = elevate(risk, RiskHigh)
		b.RiskReasons = append(b.RiskReasons, fmt.Sprintf("protection chain health factor %.2f (degraded repeater)", protection.HealthFactor))
	}
	if protection.BERFactor <= 0.8 {
		risk = elevate(risk, RiskHigh)
		b.RiskReasons = append(b.RiskReasons, fmt.Sprintf("protection BER factor %.2f indicates marginal signal", protection.BERFactor))
	}
	margin := float64(protection.HeadroomBPS) / float64(protection.EstimatedBPS)
	switch {
	case margin < 0.10:
		risk = elevate(risk, RiskElevated)
		b.RiskReasons = append(b.RiskReasons, fmt.Sprintf("protection headroom margin %.0f%% is under 10%%", margin*100))
	case margin < 0.25:
		risk = elevate(risk, RiskLow)
		b.RiskReasons = append(b.RiskReasons, fmt.Sprintf("protection headroom margin %.0f%% is thin", margin*100))
	}
	if primary.DataSufficient && primary.Overcommitted {
		risk = elevate(risk, RiskHigh)
		b.RiskReasons = append(b.RiskReasons, "primary path is already overcommitted; no safe fallback remains after the switch")
	}
	b.SwitchRisk = risk
}

// assessExpansion refuses an auto-expansion recommendation whenever evidence
// is incomplete, stale or generation-unbound. Offered headroom is the smaller
// spare of the two paths, keeping a fallback after expansion.
func assessExpansion(b *Budget) {
	primary, protection := b.Primary, b.Protection
	switch {
	case b.TopologyMismatch:
		b.Expansion = Recommendation{Allowed: false, Reason: "refused: telemetry is not bound to the current topology generation"}
		return
	case !primary.DataSufficient || !protection.DataSufficient:
		b.Expansion = Recommendation{Allowed: false, Reason: "refused: insufficient telemetry to size both primary and protection capacity"}
		return
	case b.Stale:
		b.Expansion = Recommendation{Allowed: false, Reason: "refused: telemetry is stale"}
		return
	case primary.Overcommitted || protection.Overcommitted:
		b.Expansion = Recommendation{Allowed: false, Reason: "refused: a path is already overcommitted"}
		return
	}
	spare := min64(primary.HeadroomBPS, protection.HeadroomBPS)
	if spare <= 0 {
		b.Expansion = Recommendation{Allowed: false, Reason: "refused: no positive headroom remains on the fallback path"}
		return
	}
	b.Expansion = Recommendation{
		Allowed:       true,
		ExpandableBPS: spare,
		Reason:        fmt.Sprintf("up to %d bps of additional load keeps both paths within target utilisation", spare),
	}
}

// RiskCategory is a stable, force-decision-friendly grouping of a guard result.
type RiskCategory string

const (
	RiskCategoryUncertain RiskCategory = "uncertain"
	RiskCategoryBlocked   RiskCategory = "blocked"
	RiskCategorySoft      RiskCategory = "soft"
)

// GuardError carries the human reasons, the risk category and the underlying
// budget when a switch is gated.
type GuardError struct {
	Category RiskCategory
	Level    RiskLevel
	Reasons  []string
	Budget   Budget
}

func (e *GuardError) Error() string {
	return fmt.Sprintf("capacity guard %s: %s", e.Level, joinMissing(e.Reasons))
}

// Guard gates failover on protection-path capacity.
type Guard struct {
	estimator *Estimator
}

func NewGuard(estimator *Estimator) *Guard {
	return &Guard{estimator: estimator}
}

// Check returns a GuardError when switching to protection is unsafe.
// Uncertain/blocked conditions are hard denials; elevated/high are soft and
// may be overridden by an explicit force request.
func (g *Guard) Check(in Inputs) (Budget, error) {
	budget, err := g.estimator.Evaluate(in)
	if err != nil {
		return Budget{}, err
	}
	switch budget.SwitchRisk {
	case RiskNone, RiskLow:
		return budget, nil
	case RiskElevated, RiskHigh:
		return budget, &GuardError{Category: RiskCategorySoft, Level: budget.SwitchRisk, Reasons: budget.RiskReasons, Budget: budget}
	default: // blocked, uncertain
		return budget, &GuardError{Category: RiskCategoryBlocked, Level: budget.SwitchRisk, Reasons: budget.RiskReasons, Budget: budget}
	}
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func elevate(current, candidate RiskLevel) RiskLevel {
	if riskRank(candidate) > riskRank(current) {
		return candidate
	}
	return current
}

func riskRank(level RiskLevel) int {
	switch level {
	case RiskLow:
		return 1
	case RiskElevated:
		return 2
	case RiskHigh:
		return 3
	case RiskBlocked, RiskUncertain:
		return 4
	default:
		return 0
	}
}

func joinMissing(items []string) string {
	out := ""
	for i, item := range items {
		if i > 0 {
			out += "; "
		}
		out += item
	}
	return out
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
