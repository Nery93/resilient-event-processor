package domain

import (
	"encoding/json"
	"time"
)

type EventRequest struct {
	EventID   string          `json:"event_id"`
	Payload   json.RawMessage `json:"payload"`
}

type Event struct {
	EventID   string          `json:"event_id"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp time.Time       `json:"timestamp"`
}
