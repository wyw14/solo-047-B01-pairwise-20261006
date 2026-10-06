package capacity

import "fmt"

// InputBuilder resolves a route ID to bound estimator inputs. *Builder
// satisfies it; tests may substitute a stub.
type InputBuilder interface {
	Build(routeID string) (Inputs, error)
}

// Service is the entry point used by the API and the switchover guard. It
// resolves a route to bound inputs, runs the estimator and exposes the
// generation/telemetry binding on every result.
type Service struct {
	builder   InputBuilder
	estimator *Estimator
	guard     *Guard
}

func NewService(builder InputBuilder, estimator *Estimator) *Service {
	return &Service{builder: builder, estimator: estimator, guard: NewGuard(estimator)}
}

// Budget returns the capacity budget bound to the current telemetry time and
// topology generation.
func (s *Service) Budget(routeID string) (Budget, error) {
	inputs, err := s.builder.Build(routeID)
	if err != nil {
		return Budget{}, err
	}
	return s.estimator.Evaluate(inputs)
}

// PreSwitchCheck must be consulted before a primary->protection failover. It
// returns the budget and a GuardError when the protection path cannot safely
// inherit the load. A soft risk may be overridden via Force; uncertain and
// blocked risks may not.
func (s *Service) PreSwitchCheck(routeID string) (Budget, error) {
	inputs, err := s.builder.Build(routeID)
	if err != nil {
		return Budget{}, err
	}
	return s.guard.Check(inputs)
}

// Force re-evaluates a guarded switch and permits soft risks only. A blocked
// or uncertain protection path is refused even under force, because acting on
// unbound/insufficient evidence is never safe.
func (s *Service) Force(routeID string) (Budget, error) {
	budget, err := s.PreSwitchCheck(routeID)
	if err == nil {
		return budget, nil
	}
	guardErr, ok := err.(*GuardError)
	if !ok {
		return Budget{}, err
	}
	if guardErr.Category != RiskCategorySoft {
		return budget, fmt.Errorf("%w: %s", ErrForceDenied, guardErr.Error())
	}
	return guardErr.Budget, nil
}

// ErrForceDenied wraps a hard guard rejection that cannot be overridden.
var ErrForceDenied = fmt.Errorf("capacity: force switch denied")
