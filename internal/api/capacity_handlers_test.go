package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wyw14/cry-103/internal/capacity"
)

func postJSON(t *testing.T, server *Server, target string, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, target, bytes.NewReader(raw)))
	return recorder
}

func ingestBER(t *testing.T, server *Server, repeaterID string, generation uint64, value float64) {
	t.Helper()
	recorder := postJSON(t, server, "/api/repeaters/"+repeaterID+"/telemetry", TelemetryRequest{
		Kind: "error_rate", Value: value, Unit: "", Sequence: 1, Generation: generation,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("ingest %s status=%d body=%s", repeaterID, recorder.Code, recorder.Body.String())
	}
}

func TestCapacityBudgetWithoutTelemetryRefusesSwitch(t *testing.T) {
	server := testServer(t)

	// No BER telemetry yet: budget reports insufficient data and the guard
	// blocks the failover with 412 Precondition Failed.
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/routes/R-S3/capacity-budget", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("budget status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var budget capacity.Budget
	if err := json.Unmarshal(recorder.Body.Bytes(), &budget); err != nil {
		t.Fatal(err)
	}
	if budget.Expansion.Allowed {
		t.Fatalf("expansion must be refused without telemetry: %+v", budget.Expansion)
	}
	if budget.SwitchRisk != capacity.RiskUncertain {
		t.Fatalf("risk=%s want uncertain", budget.SwitchRisk)
	}

	switchRecorder := postJSON(t, server, "/api/routes/R-S3/switchovers", SwitchoverRequest{Reason: "test"})
	if switchRecorder.Code != http.StatusPreconditionFailed {
		t.Fatalf("switch status=%d body=%s want 412", switchRecorder.Code, switchRecorder.Body.String())
	}
}

func TestCapacityBudgetWithBoundTelemetryAllowsSwitch(t *testing.T) {
	server := testServer(t)

	// Test topology registers generation 1; bind BER samples to it.
	ingestBER(t, server, "R07", 1, 0)
	ingestBER(t, server, "R08", 1, 0)

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/routes/R-S3/capacity-budget", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("budget status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var budget capacity.Budget
	if err := json.Unmarshal(recorder.Body.Bytes(), &budget); err != nil {
		t.Fatal(err)
	}
	if !budget.Primary.DataSufficient || !budget.Protection.DataSufficient {
		t.Fatalf("both paths should be sized: %+v / %+v", budget.Primary.Missing, budget.Protection.Missing)
	}
	if budget.ActiveGeneration != 1 || budget.TopologyMismatch {
		t.Fatalf("budget not bound to generation 1: %+v", budget)
	}
	if !budget.Expansion.Allowed {
		t.Fatalf("expansion should be allowed: %+v", budget.Expansion)
	}

	switchRecorder := postJSON(t, server, "/api/routes/R-S3/switchovers", SwitchoverRequest{Reason: "test"})
	if switchRecorder.Code != http.StatusAccepted {
		t.Fatalf("switch status=%d body=%s want 202", switchRecorder.Code, switchRecorder.Body.String())
	}
}

func TestCapacityBudgetGenerationMismatchBlocksSwitch(t *testing.T) {
	server := testServer(t)

	// Primary sample matches gen 1; protection sample is from an older
	// generation so the evidence cannot be bound to the current topology.
	ingestBER(t, server, "R07", 1, 0)
	ingestBER(t, server, "R08", 99, 0)

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/routes/R-S3/capacity-budget", nil))
	var budget capacity.Budget
	if err := json.Unmarshal(recorder.Body.Bytes(), &budget); err != nil {
		t.Fatal(err)
	}
	if !budget.TopologyMismatch || budget.Expansion.Allowed {
		t.Fatalf("expected generation-mismatch refusal: %+v", budget)
	}

	// Force must not override uncertain evidence.
	switchRecorder := postJSON(t, server, "/api/routes/R-S3/switchovers", SwitchoverRequest{Reason: "test", Force: true})
	if switchRecorder.Code != http.StatusPreconditionFailed {
		t.Fatalf("forced switch status=%d body=%s want 412", switchRecorder.Code, switchRecorder.Body.String())
	}
}

func TestAssignPathRepeatersRejectsUnknownRepeater(t *testing.T) {
	server := testServer(t)
	recorder := postJSON(t, server, "/api/spans/S3/path-repeaters", PathRepeatersRequest{
		Path: "protection", RepeaterIDs: []string{"GHOST"},
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s want 400", recorder.Code, recorder.Body.String())
	}
}
