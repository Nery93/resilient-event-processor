package httpapi

import "encoding/json"

type EventRequest struct {
	Payload json.RawMessage `json:"payload"`
}
