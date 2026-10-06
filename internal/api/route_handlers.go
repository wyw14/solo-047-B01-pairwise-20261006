package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/capacity"
	"github.com/wyw14/cry-103/internal/switchover"
)

func (s *Server) startSwitchover(writer http.ResponseWriter, request *http.Request) {
	routeID := chi.URLParam(request, "id")
	target, err := s.services.Route(routeID)
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input SwitchoverRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	result, err := s.services.Switchovers.Failover(request.Context(), target, input.Force)
	if err != nil {
		// A capacity refusal is a precondition failure, not an actuator error:
		// surface 412 with the operation's risk record.
		if errors.Is(err, switchover.ErrSwitchGuard) {
			writeJSON(writer, http.StatusPreconditionFailed, ErrorResponse{Error: err.Error()})
			return
		}
		writeError(writer, http.StatusConflict, err)
		return
	}
	if err := s.services.Persist("switchover", result.ID, result); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, result)
}

func (s *Server) getRoute(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Route(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, target.Snapshot())
}

// getCapacityBudget returns the primary/protection capacity budget bound to
// the latest telemetry time and the active topology generation.
func (s *Server) getCapacityBudget(writer http.ResponseWriter, request *http.Request) {
	routeID := chi.URLParam(request, "id")
	if _, err := s.services.Route(routeID); err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	if s.services.Capacity == nil {
		writeError(writer, http.StatusServiceUnavailable, errors.New("capacity budgeting is not configured"))
		return
	}
	budget, err := s.services.Capacity.Budget(routeID)
	if err != nil {
		writeError(writer, http.StatusUnprocessableEntity, err)
		return
	}
	writeJSON(writer, http.StatusOK, budget)
}

// assignPathRepeaters binds repeater chains to primary/protection so the
// estimator can size each path independently.
func (s *Server) assignPathRepeaters(writer http.ResponseWriter, request *http.Request) {
	spanID := chi.URLParam(request, "id")
	var input PathRepeatersRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	path := capacity.Path(input.Path)
	if err := s.services.AssignPathRepeaters(spanID, path, input.RepeaterIDs); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"span_id": spanID, "path": path, "repeaters": input.RepeaterIDs,
	})
}
