package switchover

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wyw14/cry-103/internal/capacity"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/route"
)

// ErrSwitchGuard marks a failover refused by the capacity guard. Callers can
// use errors.Is to distinguish a capacity refusal from an actuator failure.
var ErrSwitchGuard = errors.New("switchover refused by capacity guard")

// CapacityGate is the pre-switch risk gate. *capacity.Service satisfies it.
type CapacityGate interface {
	PreSwitchCheck(routeID string) (capacity.Budget, error)
}

type SessionService struct {
	drainer      *route.Drainer
	activator    *route.Activator
	gate         CapacityGate
	ids          platform.IDGenerator
	now          func() time.Time
	drainTimeout time.Duration
}

func NewSessionService(drainer *route.Drainer, activator *route.Activator, gate CapacityGate, ids platform.IDGenerator, now func() time.Time, drainTimeout time.Duration) *SessionService {
	return &SessionService{drainer: drainer, activator: activator, gate: gate, ids: ids, now: now, drainTimeout: drainTimeout}
}

// Failover drains the primary and moves traffic onto protection. When a
// capacity gate is configured it is consulted first: the budget is bound to
// the current telemetry time and topology generation, and a risky or
// unprovable protection path aborts the switch before any fibre is moved.
// force=true overrides soft (elevated/high) risks only; blocked capacity and
// uncertain/insufficient evidence cannot be overridden.
func (s *SessionService) Failover(ctx context.Context, target *route.Route, force bool) (Snapshot, error) {
	operation := NewOperation(s.ids.New("switch"), target.Snapshot().ID, s.now())

	if s.gate != nil {
		budget, guardErr := s.authorize(target.Snapshot().ID, force)
		if guardErr != nil {
			return operation.record(StateFailed, "capacity-guard", guardErr.Error(), s.now()), guardErr
		}
		operation.record(StateCreated, "capacity-guard",
			fmt.Sprintf("capacity budget bound to telemetry at %s and topology generation %d; protection risk=%s, headroom=%d bps",
				budget.TelemetryBoundAt.Format(time.RFC3339), budget.ActiveGeneration, budget.SwitchRisk, budget.Protection.HeadroomBPS),
			s.now())
	}

	operation.record(StateDrainingPrimary, "primary-drain", "draining failed primary path", s.now())
	drainTimeout := s.drainTimeout
	if drainTimeout <= 0 {
		drainTimeout = s.drainer.Delay()
	}
	drainCtx, cancelDrain := context.WithTimeout(ctx, drainTimeout)
	_, drainErr := s.drainer.Drain(drainCtx, target, s.now())
	cancelDrain()
	if drainErr != nil {
		operation.record(StateDrainingPrimary, "primary-drain", drainErr.Error(), s.now())
	}
	operation.record(StateActivating, "protection-activate", "activating protection path", s.now())
	if _, err := s.activator.Protection(ctx, target, s.now()); err != nil {
		result := operation.record(StateFailed, "protection-activate", err.Error(), s.now())
		return result, fmt.Errorf("activate protection: %w", err)
	}
	message := "protection path carries traffic"
	if drainErr != nil {
		message = "protection path carries traffic after primary drain timeout"
	}
	return operation.record(StateProtected, "complete", message, s.now()), nil
}

// authorize runs the guard and applies force semantics.
func (s *SessionService) authorize(routeID string, force bool) (capacity.Budget, error) {
	budget, err := s.gate.PreSwitchCheck(routeID)
	if err == nil {
		return budget, nil
	}
	var guardErr *capacity.GuardError
	if !errors.As(err, &guardErr) {
		return capacity.Budget{}, fmt.Errorf("%w: %s", ErrSwitchGuard, err.Error())
	}
	if force && guardErr.Category == capacity.RiskCategorySoft {
		// Explicit operator override of a measured-but-risky path. The forced
		// risk is still surfaced as the guard error text on the operation.
		return guardErr.Budget, nil
	}
	return guardErr.Budget, fmt.Errorf("%w: %s", ErrSwitchGuard, guardErr.Error())
}
