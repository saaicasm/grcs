// Package event defines the shared Event type that flows through the whole
// pipeline: it is accepted by the API, written to ScyllaDB, produced to a
// Redpanda topic as JSON, then consumed and inserted into ClickHouse.
package event

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Event is a single logging/telemetry event.
//
// The JSON tags define the wire format on the Redpanda topic (and the request
// body accepted by POST /events). Field order/types are mirrored by the Scylla
// `events` table and the ClickHouse analytics table.
type Event struct {
	// ID uniquely identifies the event. Generated server-side if the client
	// does not supply one, so producers can be fire-and-forget.
	ID string `json:"id"`

	// UserID is the actor the event is attributed to (partition key in Scylla).
	UserID string `json:"user_id"`

	// Type is the event/category name, e.g. "page_view", "click", "purchase".
	Type string `json:"type"`

	// Source identifies where the event originated, e.g. "web", "ios", "cron".
	Source string `json:"source"`

	// Timestamp is when the event occurred (UTC). Defaulted to receipt time if
	// the client omits it.
	Timestamp time.Time `json:"timestamp"`

	// Payload is arbitrary structured data attached to the event. Stored as a
	// JSON string in both Scylla and ClickHouse.
	Payload map[string]any `json:"payload,omitempty"`
}

// ErrInvalidEvent is returned by Validate when a required field is missing.
var ErrInvalidEvent = errors.New("invalid event")

// Normalize fills in server-side defaults: a generated ID and a receipt
// timestamp when the client did not provide them. Call before Validate.
func (e *Event) Normalize() {
	if e.ID == "" {
		e.ID = uuid.NewString()
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	} else {
		e.Timestamp = e.Timestamp.UTC()
	}
}

// Validate checks that the required fields are present. The API rejects events
// that fail validation before any write to Scylla or Redpanda.
func (e *Event) Validate() error {
	switch {
	case e.UserID == "":
		return errors.Join(ErrInvalidEvent, errors.New("user_id is required"))
	case e.Type == "":
		return errors.Join(ErrInvalidEvent, errors.New("type is required"))
	}
	return nil
}

// PayloadJSON returns the payload encoded as a JSON string, suitable for
// storing in a single Scylla/ClickHouse column. An empty payload encodes as
// "{}".
func (e *Event) PayloadJSON() (string, error) {
	if e.Payload == nil {
		return "{}", nil
	}
	b, err := json.Marshal(e.Payload)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Marshal serializes the event to JSON for the Redpanda topic.
func (e *Event) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

// Unmarshal parses a JSON-encoded event read from the Redpanda topic.
func Unmarshal(b []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(b, &e); err != nil {
		return Event{}, err
	}
	return e, nil
}
