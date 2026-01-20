package service

import (
	"testing"
	"time"

	"event-processor/consumer/service/models"

	"github.com/stretchr/testify/assert"
)

func TestValidateEventMessage_Invalid(t *testing.T) {
	service := &EventService{}

	event := &models.EventMessage{
		EventType:   "test-type",
		ClientKey:   "client-key",
		Destination: "new-client",
		Timestamp:   time.Now(),
		Message:     "This is a test message",
	}
	expectedErrorSubString := "Validation failed for: [EventID (zero value)]"

	err := service.ValidateEventMessage(event)

	assert.ErrorContains(t, err, expectedErrorSubString)
}

func TestValidateEventMessage_Valid(t *testing.T) {
	service := &EventService{}

	event := &models.EventMessage{
		EventID:     "event-123",
		EventType:   "test-type",
		ClientKey:   "client-key",
		Destination: "new-client",
		Timestamp:   time.Now(),
		Message:     "This is a test message",
	}

	err := service.ValidateEventMessage(event)

	assert.Nil(t, err)
}
