package domain

import (
	"encoding/json"
	"time"
)

type Event struct {
	EventID   string    `json:"event_id"`
	Payload   json.RawMessage    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}
