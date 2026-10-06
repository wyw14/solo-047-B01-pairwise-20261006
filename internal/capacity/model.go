// Package capacity estimates the bearable capacity of cable spans from
// physical and telemetry inputs, compares primary/protection headroom and
// reports risk before a path switch. Every estimate is bound to a telemetry
// time and a topology generation so stale or superseded data can never drive
// an automatic expansion recommendation.
package capacity

import (
	"errors"
	"time"

	"github.com/wyw14/cry-103/internal/topology"
)

// Sentinel errors. Callers should inspect these with errors.Is rather than
// matching error text.
var (
	// ErrInsufficientData is returned when a required input (path inventory,
	// repeaters, fresh telemetry) is missing. Automatic expansion advice is
	// refused whenever this error is present.
	ErrInsufficientData = errors.New("capacity: insufficient data for an estimate")
	// ErrStaleTelemetry is returned when the newest bound telemetry is older
	// than the configured freshness window.
	ErrStaleTelemetry = errors.New("capacity: telemetry is stale")
	// ErrGenerationMismatch is returned when an estimate is evaluated against
	// a topology generation other than the one the inputs were bound to.
	ErrGenerationMismatch = errors.New("capacity: topology generation mismatch")
)

// Config holds the physical engineering assumptions shared by every estimate.
type Config struct {
	// BaseBPS is the engineered capacity of one fibre path at zero length.
	BaseBPS int64
	// LengthAttenuationDBPerKM is the fibre attenuation coefficient used to
	// derive the length penalty.
	LengthAttenuationDBPerKM float64
	// BERFloor is the bit error rate at/above which no usable capacity can be
	// claimed (hard outage).
	BERFloor float64
	// BERTarget is the reference BER the engineered BaseBPS assumes.
	BERTarget float64
	// MaxTelemetryAge bounds how old bound telemetry may be before an
	// estimate is rejected.
	MaxTelemetryAge time.Duration
}

// DefaultConfig returns conservative single-fibre engineering defaults.
func DefaultConfig() Config {
	return Config{
		BaseBPS:                  400_000_000_000,
		LengthAttenuationDBPerKM: 0.2,
		BERFloor:                 1e-3,
		BERTarget:                1e-12,
	}
}

// Validate enforces positive engineering parameters.
func (c Config) Validate() error {
	if c.BaseBPS <= 0 {
		return errors.New("capacity: base capacity must be positive")
	}
	if c.LengthAttenuationDBPerKM <= 0 {
		return errors.New("capacity: attenuation coefficient must be positive")
	}
	if c.BERFloor <= 0 || c.BERTarget <= 0 || c.BERTarget >= c.BERFloor {
		return errors.New("capacity: require 0 < BER target < BER floor")
	}
	if c.MaxTelemetryAge <= 0 {
		return errors.New("capacity: telemetry freshness window must be positive")
	}
	return nil
}

// RepeaterState is the minimum health projection the estimator needs from a
// submerged repeater. It mirrors repeater.Snapshot without importing it so
// the domain stays decoupled from the projector package.
type RepeaterState struct {
	ID     string `json:"id"`
	Health string `json:"health"`
}

// PathSpec describes one fibre path (primary or protection) of a span: its
// physical length, the repeaters riding it and the telemetry bound to it.
type PathSpec struct {
	Path      topology.Path   `json:"path"`
	LengthKM  float64         `json:"length_km"`
	Repeaters []RepeaterState `json:"repeaters"`
	// BER is the most recent bit error rate observed on the path, observed at
	// BERObservedAt and tagged with BERGeneration. Zero with a zero time means
	// "no BER telemetry".
	BER           float64   `json:"ber"`
	BERObservedAt time.Time `json:"ber_observed_at"`
	BERGeneration uint64    `json:"ber_generation"`
}

// Inventory is the physical/telemetry picture for both paths of one span,
// frozen at a topology generation.
type Inventory struct {
	SpanID     string   `json:"span_id"`
	Generation uint64   `json:"generation"`
	Primary    PathSpec `json:"primary"`
	Protection PathSpec `json:"protection"`
}

// PathBudget is the capacity estimate for a single path.
type PathBudget struct {
	Path             topology.Path `json:"path"`
	LengthKM         float64       `json:"length_km"`
	EstimatedBPS     int64         `json:"estimated_bps"`
	LengthFactor     float64       `json:"length_factor"`
	BERFactor        float64       `json:"ber_factor"`
	RepeaterFactor   float64       `json:"repeater_factor"`
	TrippedRepeaters []string      `json:"tripped_repeaters"`
	BER              float64       `json:"ber"`
	// DataGaps lists missing inputs. A non-empty slice means the figure is a
	// conservative bound rather than a fully evidenced estimate.
	DataGaps []string `json:"data_gaps"`
}

// HasDataGap reports whether any required signal was missing.
func (b PathBudget) HasDataGap() bool { return len(b.DataGaps) > 0 }

// BudgetReport compares both paths. It is immutable evidence: the telemetry
// time and topology generation it was bound to travel with every figure.
type BudgetReport struct {
	SpanID      string        `json:"span_id"`
	Generation  uint64        `json:"generation"`
	AsOf        time.Time     `json:"as_of"`
	Primary     PathBudget    `json:"primary"`
	Protection  PathBudget    `json:"protection"`
	ActivePath  topology.Path `json:"active_path"`
	StandbyPath topology.Path `json:"standby_path"`
	Active      PathBudget    `json:"active"`
	Standby     PathBudget    `json:"standby"`
	TrafficBPS  int64         `json:"traffic_bps"`
	// Insufficient is true when any mandatory input was missing. No automatic
	// expansion recommendation may be produced from such a report.
	Insufficient bool `json:"insufficient"`
	// Reasons explains why the report is insufficient / stale.
	Reasons []string `json:"reasons"`
}

// HeadroomBPS is the spare capacity on the standby path after current traffic.
func (r BudgetReport) HeadroomBPS() int64 { return r.Standby.EstimatedBPS - r.TrafficBPS }

// HeadroomRatio is spare capacity divided by current traffic; negative means
// the standby path cannot absorb the current load.
func (r BudgetReport) HeadroomRatio() float64 {
	if r.TrafficBPS <= 0 {
		return 0
	}
	return float64(r.HeadroomBPS()) / float64(r.TrafficBPS)
}
