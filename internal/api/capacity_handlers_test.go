package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wyw14/cry-103/internal/capacity"
)

func TestCapacityBudgetAndSwitchRiskFlow(t *testing.T) {
	server := testServer(t)

	budget := httptest.NewRecorder()
	server.Handler().ServeHTTP(budget, httptest.NewRequest(http.MethodGet, "/api/spans/S3/capacity-budget", nil))
	if budget.Code != http.StatusOK {
		t.Fatalf("budget status=%d body=%s", budget.Code, budget.Body.String())
	}
	var report capacity.BudgetReport
	if err := json.Unmarshal(budget.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Generation != 1 || report.Insufficient {
		t.Fatalf("seeded inventory should yield a complete report: %+v", report)
	}
	if report.Active.Path != "primary" || report.Standby.Path != "protection" {
		t.Fatalf("unexpected active/standby paths: %+v", report)
	}

	risk := httptest.NewRecorder()
	server.Handler().ServeHTTP(risk, httptest.NewRequest(http.MethodGet, "/api/spans/S3/switch-risk", nil))
	if risk.Code != http.StatusOK {
		t.Fatalf("switch-risk status=%d body=%s", risk.Code, risk.Body.String())
	}
	var assessment capacity.SwitchAssessment
	if err := json.Unmarshal(risk.Body.Bytes(), &assessment); err != nil {
		t.Fatal(err)
	}
	if assessment.Blocked || assessment.Level != capacity.RiskSafe {
		t.Fatalf("healthy standby must be safe to switch to: %+v", assessment)
	}
	if assessment.StandbyHeadroomBPS <= 0 {
		t.Fatalf("standby margin must be reported and positive: %+v", assessment)
	}
}

func TestExpansionApprovedThenRefusedWhenTelemetryRemoved(t *testing.T) {
	server := testServer(t)

	approve := httptest.NewRecorder()
	body, _ := json.Marshal(ExpansionRequest{RequestedBPS: 1000})
	server.Handler().ServeHTTP(approve,
		httptest.NewRequest(http.MethodPost, "/api/spans/S3/expansion-requests", bytes.NewReader(body)))
	if approve.Code != http.StatusOK {
		t.Fatalf("expansion status=%d body=%s", approve.Code, approve.Body.String())
	}
	var advice capacity.ExpansionAdvice
	if err := json.Unmarshal(approve.Body.Bytes(), &advice); err != nil {
		t.Fatal(err)
	}
	if !advice.Approved || len(advice.Candidates) == 0 {
		t.Fatalf("complete evidence should approve expansion: %+v", advice)
	}

	// Rebind without BER telemetry: automatic advice must be refused, not
	// guessed from an optimistic default.
	incomplete, _ := json.Marshal(CapacityInventoryRequest{
		Primary:    CapacityPathInput{LengthKM: 126, RepeaterIDs: []string{"R07"}},
		Protection: CapacityPathInput{LengthKM: 132, BER: 1e-9},
	})
	rebind := httptest.NewRecorder()
	server.Handler().ServeHTTP(rebind,
		httptest.NewRequest(http.MethodPut, "/api/spans/S3/capacity-inventory", bytes.NewReader(incomplete)))
	if rebind.Code != http.StatusOK {
		t.Fatalf("inventory bind status=%d body=%s", rebind.Code, rebind.Body.String())
	}

	refused := httptest.NewRecorder()
	server.Handler().ServeHTTP(refused,
		httptest.NewRequest(http.MethodPost, "/api/spans/S3/expansion-requests", bytes.NewReader(body)))
	if refused.Code != http.StatusConflict {
		t.Fatalf("missing telemetry must refuse with 409, got %d body=%s", refused.Code, refused.Body.String())
	}
	if err := json.Unmarshal(refused.Body.Bytes(), &advice); err != nil {
		t.Fatal(err)
	}
	if advice.Approved || len(advice.Reasons) == 0 {
		t.Fatalf("refusal must be explicit and carry reasons: %+v", advice)
	}

	risk := httptest.NewRecorder()
	server.Handler().ServeHTTP(risk, httptest.NewRequest(http.MethodGet, "/api/spans/S3/switch-risk", nil))
	var blocked capacity.SwitchAssessment
	if err := json.Unmarshal(risk.Body.Bytes(), &blocked); err != nil {
		t.Fatal(err)
	}
	if !blocked.Blocked {
		t.Fatalf("a switch must be withheld while capacity evidence is incomplete: %+v", blocked)
	}
}

func TestCapacityBudgetRejectsUnknownSpan(t *testing.T) {
	server := testServer(t)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/spans/NOPE/capacity-budget", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown span must be 404, got %d", response.Code)
	}
}
