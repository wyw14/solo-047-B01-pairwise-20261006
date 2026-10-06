package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/capacity"
	"github.com/wyw14/cry-103/internal/topology"
)

// routeForSpan returns the route carrying a span, if one is registered.
func (s *Server) routeForSpan(spanID string) (string, bool) {
	s.services.mu.RLock()
	defer s.services.mu.RUnlock()
	for id, candidate := range s.services.Routes {
		if candidate.Snapshot().SpanID == spanID {
			return id, true
		}
	}
	return "", false
}

func (s *Server) putCapacityInventory(writer http.ResponseWriter, request *http.Request) {
	spanID := chi.URLParam(request, "id")
	if _, err := s.services.Span(spanID); err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input CapacityInventoryRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	build := func(path topology.Path, entry CapacityPathInput) (capacity.PathSpec, error) {
		spec := capacity.PathSpec{
			Path: path, LengthKM: entry.LengthKM, BER: entry.BER,
			BERObservedAt: entry.ObservedAt.UTC(),
		}
		if spec.BERObservedAt.IsZero() && spec.BER > 0 {
			spec.BERObservedAt = s.services.Clock.Now().UTC()
		}
		for _, repeaterID := range entry.RepeaterIDs {
			unit, err := s.services.Repeater(repeaterID)
			if err != nil {
				return capacity.PathSpec{}, err
			}
			spec.Repeaters = append(spec.Repeaters, capacity.RepeaterState{
				ID: repeaterID, Health: string(unit.Snapshot().Health),
			})
		}
		return spec, nil
	}
	primary, err := build(topology.PathPrimary, input.Primary)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	protection, err := build(topology.PathProtection, input.Protection)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	inventory, err := s.services.Capacity.Bind(spanID, primary, protection)
	if err != nil {
		writeCapacityError(writer, err)
		return
	}
	if err := s.services.Persist("capacity-inventory", spanID, inventory); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, inventory)
}

func (s *Server) getCapacityBudget(writer http.ResponseWriter, request *http.Request) {
	spanID := chi.URLParam(request, "id")
	if _, err := s.services.Span(spanID); err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	traffic := s.trafficBPS(request, spanID)
	report, err := s.services.Capacity.Report(spanID, traffic)
	if err != nil && report.SpanID == "" {
		writeCapacityError(writer, err)
		return
	}
	// A populated report is evidence even when flagged insufficient; the
	// insufficient/reasons fields carry the refusal explanation.
	writeJSON(writer, http.StatusOK, report)
}

func (s *Server) getSwitchRisk(writer http.ResponseWriter, request *http.Request) {
	spanID := chi.URLParam(request, "id")
	if _, err := s.services.Span(spanID); err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	traffic := s.trafficBPS(request, spanID)
	assessment, err := s.services.Capacity.AssessSwitch(spanID, traffic)
	if assessment.SpanID == "" {
		writeCapacityError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, assessment)
}

func (s *Server) requestExpansion(writer http.ResponseWriter, request *http.Request) {
	spanID := chi.URLParam(request, "id")
	if _, err := s.services.Span(spanID); err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input ExpansionRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	advice, err := s.services.Capacity.RecommendExpansion(spanID, input.RequestedBPS)
	if err != nil {
		// Automatic expansion is refused; the body explains the data gap.
		writeJSON(writer, http.StatusConflict, advice)
		return
	}
	if err := s.services.Persist("capacity-expansion", spanID, advice); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, advice)
}

func (s *Server) trafficBPS(request *http.Request, spanID string) int64 {
	if raw := request.URL.Query().Get("traffic_bps"); raw != "" {
		if value, err := strconv.ParseInt(raw, 10, 64); err == nil && value >= 0 {
			return value
		}
	}
	if routeID, ok := s.routeForSpan(spanID); ok {
		if target, err := s.services.Route(routeID); err == nil {
			return target.Snapshot().TrafficBPS
		}
	}
	return 0
}

func writeCapacityError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, capacity.ErrGenerationMismatch), errors.Is(err, capacity.ErrStaleTelemetry):
		writeError(writer, http.StatusConflict, err)
	case errors.Is(err, capacity.ErrInsufficientData):
		writeError(writer, http.StatusUnprocessableEntity, err)
	default:
		writeError(writer, http.StatusBadRequest, err)
	}
}
