package models

import (
	"time"
)

type EventMessage struct {
	EventID     string      `validate:"nonzero" json:"event_id" dynamodbav:"event_id"`
	EventType   string      `validate:"nonzero" json:"event_type" dynamodbav:"event_type"`
	ClientKey   string      `validate:"nonzero" json:"client_key" dynamodbav:"client_key"`
	Destination string      `validate:"nonzero" json:"destination" dynamodbav:"destination"`
	Timestamp   time.Time   `json:"timestamp" dynamodbav:"timestamp"`
	Message     interface{} `validate:"nonzero" json:"message" dynamodbav:"message"`
}

type EventLog struct {
	EventID      string      `json:"event_id" dynamodbav:"event_id"`
	EventMessage interface{} `json:"event_message"`
	Status       string      `json:"status"`
	Details      string      `json:"details"`
	Timestamp    time.Time   `json:"timestamp" dynamodbav:"timestamp"`
}
