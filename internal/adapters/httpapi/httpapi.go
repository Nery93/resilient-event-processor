package httpapi

import (
	"encoding/json"
	"net/http"
	"time"
	
	"github.com/google/uuid"
	"resilient-event-processor/internal/domain"
	"resilient-event-processor/internal/ports"
)

type Handler struct {
	eventPublisher ports.EventPublisher
}

func (h *Handler) PublishEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req EventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if len(req.Payload) == 0 {
		http.Error(w, "payload is required", http.StatusBadRequest)
		return
	}

	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		idempotencyKey = uuid.NewString()
	}

	event := domain.Event{
		EventID:   idempotencyKey,
		Payload:   req.Payload,
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
