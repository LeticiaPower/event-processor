package service

import (
	"context"
	"errors"
	"fmt"
	"log"

	"event-processor/config"
	"event-processor/consumer/service/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"gopkg.in/validator.v2"
)

type EventService struct {
	db *dynamodb.Client
}

func NewEventService(db *dynamodb.Client) *EventService {
	return &EventService{db: db}
}

func (s *EventService) ValidateEventMessage(eventMessage *models.EventMessage) error {
	if err := validator.Validate(eventMessage); err != nil {
		errs := err.(validator.ErrorMap)
		var errOut []string
		for f, e := range errs {
			errOut = append(errOut, fmt.Sprintf("%s (%v)", f, e))
		}

		return fmt.Errorf("Validation failed for: %s", errOut)
	}
	return nil
}

func (s *EventService) CreateEventMessage(ctx context.Context, eventMessage *models.EventMessage) error {
	existingEvent, err := s.GetEventMessageByEventID(ctx, eventMessage.EventID)
	if err == nil && existingEvent != nil {
		return nil
	} else if err != nil && err.Error() != "Event not found" {
		return err
	}

	item, err := attributevalue.MarshalMap(eventMessage)
	if err != nil {
		return fmt.Errorf("Failed to parse event message: %w", err)
	}
	_, err = s.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(config.EventTable),
		Item:      item,
	})
	if err != nil {
		log.Println("Failed to create event:", err)
		return fmt.Errorf("Failed to create event: %w", err)
	}

	log.Printf("Event with ID %s created with success", eventMessage.EventID)
	return nil
}

func (s *EventService) GetEventMessageByEventID(ctx context.Context, eventID string) (*models.EventMessage, error) {
	resp, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(config.EventTable),
		Key:       map[string]types.AttributeValue{"event_id": &types.AttributeValueMemberS{Value: eventID}},
	})

	if err != nil {
		return nil, fmt.Errorf("Failed to get event message: %w", err)
	}

	if resp.Item == nil {
		return nil, errors.New("Event not found")
	}

	var eventMessage models.EventMessage
	attributevalue.UnmarshalMap(resp.Item, &eventMessage)

	return &eventMessage, nil
}

func (s *EventService) LogEventStatus(ctx context.Context, eventLog *models.EventLog) error {
	item, _ := attributevalue.MarshalMap(eventLog)

	_, err := s.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(config.EventLogsTable),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("Failed to log event: %w", err)
	}

	log.Printf("Log Saved for event: %s - %s", eventLog.EventID, eventLog.Status)
	return nil
}
