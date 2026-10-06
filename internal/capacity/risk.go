package capacity

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/topology"
)

// RiskLevel is the pre-switch verdict for the standby path.
type RiskLevel string

const (
	RiskSafe    RiskLevel = "safe"
	RiskCaution RiskLevel = "caution"
	RiskHigh    RiskLevel = "high"
	RiskBlocked RiskLevel = "blocked"
)

// cautionHeadroomRatio is the minimum spare fraction (headroom/traffic) a
// standby path should hold before a switch is considered routine.
const cautionHeadroomRatio = 0.15

// ampleHeadroomRatio is the spare fraction above which a smaller standby
// margin than the active path is merely informational, not a downgrade.
const ampleHeadroomRatio = 1.0

// TopologyView is the subset of topology.Registry the service needs.
type TopologyView interface {
	Snapshot(spanID string) (topology.Snapshot, error)
}

// InventoryStore keeps the newest bound inventory per span. It is safe for
// concurrent use.
type InventoryStore struct {
	mu    sync.RWMutex
	items map[string]Inventory
}

func NewInventoryStore() *InventoryStore {
	return &InventoryStore{items: make(map[string]Inventory)}
}

// Put replaces the inventory for a span only when the generation advances;
// an older generation must never overwrite a newer physical picture.
func (s *InventoryStore) Put(in Inventory) error {
	if err := in.validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, exists := s.items[in.SpanID]; exists && in.Generation < current.Generation {
		return fmt.Errorf("%w: inventory generation %d is older than %d for span %s",
			ErrGenerationMismatch, in.Generation, current.Generation, in.SpanID)
	}
	s.items[in.SpanID] = in
	return nil
}

func (s *InventoryStore) Get(spanID string) (Inventory, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	in, exists := s.items[spanID]
	return in, exists
}

// SwitchAssessment compares the active and standby margins and states the
// risk of moving traffic now.
type SwitchAssessment struct {
	SpanID             string        `json:"span_id"`
	Generation         uint64        `json:"generation"`
	LiveGeneration     uint64        `json:"live_generation"`
	AsOf               time.Time     `json:"as_of"`
	ActivePath         topology.Path `json:"active_path"`
	StandbyPath        topology.Path `json:"standby_path"`
	TrafficBPS         int64         `json:"traffic_bps"`
	ActiveCapacityBPS  int64         `json:"active_capacity_bps"`
	StandbyCapacityBPS int64         `json:"standby_capacity_bps"`
	ActiveHeadroomBPS  int64         `json:"active_headroom_bps"`
	StandbyHeadroomBPS int64         `json:"standby_headroom_bps"`
	HeadroomRatio      float64       `json:"headroom_ratio"`
	Level              RiskLevel     `json:"level"`
	Recommendation     string        `json:"recommendation"`
	Risks              []string      `json:"risks"`
	Blocked            bool          `json:"blocked"`
	Insufficient       bool          `json:"insufficient"`
	Reasons            []string      `json:"reasons,omitempty"`
}

// ExpansionAdvice is the only automatic expansion output. It is refused
// unless every figure is fully evidenced at the live topology generation.
type ExpansionAdvice struct {
	SpanID         string          `json:"span_id"`
	Generation     uint64          `json:"generation"`
	LiveGeneration uint64          `json:"live_generation"`
	AsOf           time.Time       `json:"as_of"`
	RequestedBPS   int64           `json:"requested_bps"`
	Approved       bool            `json:"approved"`
	Decision       string          `json:"decision"`
	Candidates     []PathExpansion `json:"candidates"`
	Reasons        []string        `json:"reasons,omitempty"`
}

// PathExpansion names one path that can absorb the requested load and the
// headroom left afterwards.
type PathExpansion struct {
	Path                     topology.Path `json:"path"`
	EstimatedBPS             int64         `json:"estimated_bps"`
	PostExpansionHeadroomBPS int64         `json:"post_expansion_headroom_bps"`
}

// Service binds physical inventories to the live topology and produces
// generation-stamped, telemetry-time-stamped advice.
type Service struct {
	estimator *Estimator

	topologies  TopologyView
	inventories *InventoryStore
	now         func() time.Time
}

func NewService(estimator *Estimator, topologies TopologyView, inventories *InventoryStore, now func() time.Time) (*Service, error) {
	if estimator == nil || topologies == nil || inventories == nil || now == nil {
		return nil, errors.New("capacity: estimator, topology, inventory store and clock are required")
	}
	return &Service{estimator: estimator, topologies: topologies, inventories: inventories, now: now}, nil
}

// Bind freezes a physical/telemetry picture for a span onto the *live*
// topology generation. BER observations without their own generation are
// stamped with that generation because the binding is happening now; an
// observation that already carries a different generation is preserved so the
// estimator can flag the drift. Older generations are rejected by the store.
func (s *Service) Bind(spanID string, primary, protection PathSpec) (Inventory, error) {
	live, err := s.topologies.Snapshot(spanID)
	if err != nil {
		return Inventory{}, fmt.Errorf("%w: topology: %v", ErrInsufficientData, err)
	}
	primary.Path = topology.PathPrimary
	protection.Path = topology.PathProtection
	stamp := func(spec PathSpec) PathSpec {
		if spec.BER > 0 && !spec.BERObservedAt.IsZero() && spec.BERGeneration == 0 {
			spec.BERGeneration = live.Generation
		}
		return spec
	}
	in := Inventory{
		SpanID: spanID, Generation: live.Generation,
		Primary: stamp(primary), Protection: stamp(protection),
	}
	if err := s.inventories.Put(in); err != nil {
		return Inventory{}, err
	}
	return in, nil
}

// Report returns the bound margin comparison for a span. When the inventory
// generation is behind the live topology the report is still populated (for
// display) but returned with ErrGenerationMismatch and flagged insufficient.
func (s *Service) Report(spanID string, trafficBPS int64) (BudgetReport, error) {
	live, err := s.topologies.Snapshot(spanID)
	if err != nil {
		return BudgetReport{}, fmt.Errorf("%w: topology: %v", ErrInsufficientData, err)
	}
	in, exists := s.inventories.Get(spanID)
	if !exists {
		return BudgetReport{}, fmt.Errorf("%w: no capacity inventory bound for span %s", ErrInsufficientData, spanID)
	}
	report, reportErr := s.estimator.Report(in, live.ActivePath, trafficBPS, s.now())
	if in.Generation != live.Generation {
		report.Insufficient = true
		report.Reasons = append(report.Reasons, fmt.Sprintf(
			"inventory is bound to topology generation %d but live generation is %d", in.Generation, live.Generation))
		reportErr = errors.Join(reportErr, fmt.Errorf("%w: inventory generation %d, live %d",
			ErrGenerationMismatch, in.Generation, live.Generation))
	}
	return report, reportErr
}

// AssessSwitch evaluates the risk of moving current traffic to the standby
// path. The assessment is always returned when an inventory exists (for
// display); Blocked is the hard gate an automated switch must respect.
func (s *Service) AssessSwitch(spanID string, trafficBPS int64) (SwitchAssessment, error) {
	live, err := s.topologies.Snapshot(spanID)
	if err != nil {
		return SwitchAssessment{}, fmt.Errorf("%w: topology: %v", ErrInsufficientData, err)
	}
	report, reportErr := s.Report(spanID, trafficBPS)
	assessment := SwitchAssessment{
		SpanID: spanID, LiveGeneration: live.Generation, AsOf: s.now().UTC(),
		TrafficBPS: trafficBPS, Level: RiskSafe, Recommendation: "proceed",
	}
	if report.SpanID != "" {
		assessment = s.fromReport(report)
		assessment.LiveGeneration = live.Generation
	}
	if reportErr != nil {
		switch {
		case errors.Is(reportErr, ErrGenerationMismatch):
			assessment.block("topology generation mismatch: capacity evidence predates a topology change", RiskBlocked)
		case errors.Is(reportErr, ErrStaleTelemetry):
			assessment.block("telemetry is stale: capacity evidence is too old to switch on", RiskBlocked)
		default:
			assessment.block("capacity evidence is incomplete; automatic switch withheld", RiskBlocked)
		}
		assessment.Reasons = report.Reasons
		return assessment, reportErr
	}
	s.classify(&assessment, report)
	return assessment, nil
}

func (s *Service) fromReport(report BudgetReport) SwitchAssessment {
	return SwitchAssessment{
		SpanID:             report.SpanID,
		Generation:         report.Generation,
		AsOf:               report.AsOf,
		ActivePath:         report.ActivePath,
		StandbyPath:        report.StandbyPath,
		TrafficBPS:         report.TrafficBPS,
		ActiveCapacityBPS:  report.Active.EstimatedBPS,
		StandbyCapacityBPS: report.Standby.EstimatedBPS,
		ActiveHeadroomBPS:  report.Active.EstimatedBPS - report.TrafficBPS,
		StandbyHeadroomBPS: report.HeadroomBPS(),
		HeadroomRatio:      report.HeadroomRatio(),
		Level:              RiskSafe,
		Recommendation:     "proceed",
		Insufficient:       report.Insufficient,
		Reasons:            report.Reasons,
	}
}

func (a *SwitchAssessment) block(message string, level RiskLevel) {
	a.Blocked = true
	a.Level = level
	a.Risks = append(a.Risks, message)
	a.Recommendation = "hold"
}

// classify applies the margin/health rules once evidence is complete.
func (s *Service) classify(a *SwitchAssessment, report BudgetReport) {
	standby := report.Standby
	if standby.EstimatedBPS <= 0 {
		a.block(fmt.Sprintf("%s path has zero bearable capacity: %s",
			standby.Path, standbyOutage(standby)), RiskBlocked)
		return
	}
	if a.StandbyHeadroomBPS < 0 {
		a.block(fmt.Sprintf("%s path can bear %d bps, %d bps less than current traffic",
			standby.Path, standby.EstimatedBPS, -a.StandbyHeadroomBPS), RiskHigh)
	} else if a.HeadroomRatio < cautionHeadroomRatio {
		a.Risks = append(a.Risks, fmt.Sprintf(
			"thin margin on %s path: %d bps headroom (%.0f%% of traffic)",
			standby.Path, a.StandbyHeadroomBPS, a.HeadroomRatio*100))
		a.Level = RiskCaution
		a.Recommendation = "proceed-with-caution"
	}
	if len(standby.TrippedRepeaters) > 0 {
		a.block(fmt.Sprintf("%s path repeaters latched off: %v", standby.Path, standby.TrippedRepeaters), RiskBlocked)
	}
	if standby.RepeaterFactor < 1 && standby.RepeaterFactor > 0 {
		a.Risks = append(a.Risks, fmt.Sprintf(
			"%s path carries over-temperature repeaters (health factor %.2f)", standby.Path, standby.RepeaterFactor))
		if a.Level == RiskSafe {
			a.Level = RiskCaution
			a.Recommendation = "proceed-with-caution"
		}
	}
	if standby.BERFactor < 1 {
		a.Risks = append(a.Risks, fmt.Sprintf(
			"%s path error rate is elevated (BER factor %.2f, BER %.2e)", standby.Path, standby.BERFactor, standby.BER))
		if a.Level == RiskSafe {
			a.Level = RiskCaution
			a.Recommendation = "proceed-with-caution"
		}
	}
	if a.ActiveHeadroomBPS > a.StandbyHeadroomBPS && a.HeadroomRatio < ampleHeadroomRatio {
		a.Risks = append(a.Risks, fmt.Sprintf(
			"%s standby path offers %d bps less headroom than the active path",
			standby.Path, a.ActiveHeadroomBPS-a.StandbyHeadroomBPS))
		if a.Level == RiskSafe {
			a.Level = RiskCaution
			a.Recommendation = "proceed-with-caution"
		}
	}
	if a.ActiveHeadroomBPS > a.StandbyHeadroomBPS && a.HeadroomRatio >= ampleHeadroomRatio {
		a.Risks = append(a.Risks, fmt.Sprintf(
			"%s standby path offers %d bps less headroom than active but retains an ample %.0f%% margin",
			standby.Path, a.ActiveHeadroomBPS-a.StandbyHeadroomBPS, a.HeadroomRatio*100))
	}
}

func standbyOutage(budget PathBudget) string {
	switch {
	case len(budget.TrippedRepeaters) > 0:
		return fmt.Sprintf("tripped repeaters %v", budget.TrippedRepeaters)
	case budget.BERFactor == 0:
		return fmt.Sprintf("error rate %.2e at or above outage floor", budget.BER)
	default:
		return "physical penalties consume all usable capacity"
	}
}

// RecommendExpansion returns automatic expansion guidance only when evidence
// is complete and current. Any data gap, stale telemetry or generation drift
// refuses the recommendation outright; callers must surface the reasons and
// must not expand capacity on their own.
func (s *Service) RecommendExpansion(spanID string, requestedBPS int64) (ExpansionAdvice, error) {
	if requestedBPS <= 0 {
		return ExpansionAdvice{}, errors.New("capacity: requested expansion must be positive")
	}
	live, err := s.topologies.Snapshot(spanID)
	if err != nil {
		return ExpansionAdvice{}, fmt.Errorf("%w: topology: %v", ErrInsufficientData, err)
	}
	report, reportErr := s.Report(spanID, 0)
	advice := ExpansionAdvice{
		SpanID: spanID, LiveGeneration: live.Generation, AsOf: s.now().UTC(),
		RequestedBPS: requestedBPS, Decision: "refused",
	}
	if report.SpanID != "" {
		advice.Generation = report.Generation
		advice.AsOf = report.AsOf
	}
	if reportErr != nil {
		advice.Reasons = append(advice.Reasons, refuseReasons(reportErr, report)...)
		return advice, fmt.Errorf("capacity: automatic expansion refused: %w", reportErr)
	}
	for _, budget := range []PathBudget{report.Primary, report.Protection} {
		if budget.EstimatedBPS >= requestedBPS {
			advice.Candidates = append(advice.Candidates, PathExpansion{
				Path: budget.Path, EstimatedBPS: budget.EstimatedBPS,
				PostExpansionHeadroomBPS: budget.EstimatedBPS - requestedBPS,
			})
		}
	}
	if len(advice.Candidates) == 0 {
		advice.Reasons = append(advice.Reasons, fmt.Sprintf(
			"neither path can bear an additional %d bps at generation %d", requestedBPS, report.Generation))
		return advice, nil
	}
	advice.Approved = true
	advice.Decision = fmt.Sprintf("expand-eligible:%s", advice.Candidates[0].Path)
	return advice, nil
}

func refuseReasons(reportErr error, report BudgetReport) []string {
	reasons := append([]string(nil), report.Reasons...)
	if len(reasons) == 0 {
		reasons = append(reasons, reportErr.Error())
	}
	return reasons
}
