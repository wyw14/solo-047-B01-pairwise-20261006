package switchover

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/capacity"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/topology"
)

type fakeGate struct {
	budget capacity.Budget
	err    error
	calls  int
	forced []bool
}

func (g *fakeGate) PreSwitchCheck(routeID string) (capacity.Budget, error) {
	g.calls++
	return g.budget, g.err
}

func newTestRoute(t *testing.T, now time.Time) *route.Route {
	t.Helper()
	target, err := route.New("R-S3", "S3", now)
	if err != nil {
		t.Fatal(err)
	}
	return target
}

func TestFailoverBlockedByGuardDoesNotTouchRoute(t *testing.T) {
	now := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, now); err != nil {
		t.Fatal(err)
	}
	target := newTestRoute(t, now)
	gate := &fakeGate{
		budget: capacity.Budget{SwitchRisk: capacity.RiskBlocked},
		err:    &capacity.GuardError{Category: capacity.RiskCategoryBlocked, Level: capacity.RiskBlocked, Reasons: []string{"protection short"}},
	}
	service := NewSessionService(route.NewDrainer(0), route.NewActivator(registry), gate, platform.UUIDGenerator{}, func() time.Time { return now }, time.Second)

	snapshot, err := service.Failover(context.Background(), target, false)
	if !errors.Is(err, ErrSwitchGuard) {
		t.Fatalf("want ErrSwitchGuard got %v", err)
	}
	if snapshot.State != StateFailed {
		t.Fatalf("operation state=%s want failed", snapshot.State)
	}
	if got := target.Snapshot().State; got != route.StatePrimary {
		t.Fatalf("route must stay primary after guard refusal, got %s", got)
	}
}

func TestFailoverSoftRiskRequiresForce(t *testing.T) {
	now := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, now); err != nil {
		t.Fatal(err)
	}
	target := newTestRoute(t, now)
	gate := &fakeGate{
		budget: capacity.Budget{SwitchRisk: capacity.RiskElevated, ActiveGeneration: 3},
		err:    &capacity.GuardError{Category: capacity.RiskCategorySoft, Level: capacity.RiskElevated, Reasons: []string{"thin margin"}},
	}
	service := NewSessionService(route.NewDrainer(0), route.NewActivator(registry), gate, platform.UUIDGenerator{}, func() time.Time { return now }, time.Second)

	if _, err := service.Failover(context.Background(), target, false); !errors.Is(err, ErrSwitchGuard) {
		t.Fatalf("soft risk without force must be refused, got %v", err)
	}
	snapshot, err := service.Failover(context.Background(), target, true)
	if err != nil {
		t.Fatalf("force must override soft risk: %v", err)
	}
	if snapshot.State != StateProtected {
		t.Fatalf("state=%s want protected", snapshot.State)
	}
	if got := target.Snapshot().ActivePath; got != topology.PathProtection {
		t.Fatalf("active path=%s want protection", got)
	}
}

func TestFailoverWithoutGateBehavesAsBefore(t *testing.T) {
	now := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, now); err != nil {
		t.Fatal(err)
	}
	target := newTestRoute(t, now)
	service := NewSessionService(route.NewDrainer(0), route.NewActivator(registry), nil, platform.UUIDGenerator{}, func() time.Time { return now }, time.Second)
	snapshot, err := service.Failover(context.Background(), target, false)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State != StateProtected {
		t.Fatalf("state=%s want protected", snapshot.State)
	}
}
