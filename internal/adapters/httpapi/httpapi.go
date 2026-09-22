package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"resilient-event-processor/internal/domain"
	"resilient-event-processor/internal/ports"
)

type Handler struct {
	eventPublisher ports.EventPublisher
}


func (h *Handler) PublishEvent(w http.ResponseWriter, r *http.Request) {
	var req domain.EventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	event := domain.Event{
		EventID: req.EventID,
		Payload: req.Payload,
		Timestamp: time.Now().UTC(),
	}
	if err := h.eventPublisher.Publish(r.Context(), event); err != nil {
		http.Error(w, "failed to publish event", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func NewHandler(eventPublisher ports.EventPublisher) *Handler {
	return &Handler{
		eventPublisher: eventPublisher,
	}
}
