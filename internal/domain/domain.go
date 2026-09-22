package domain

import (
	"encoding/json"
)

type EventRequest struct {
	EventID   string          `json:"event_id"`
	Payload   json.RawMessage `json:"payload"`
}
