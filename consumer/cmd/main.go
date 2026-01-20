package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"event-processor/config"
	"event-processor/consumer/event"
	awsClient "event-processor/consumer/pkg/aws"
	"event-processor/consumer/service"
	"event-processor/consumer/service/models"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func initDynamoDB() *dynamodb.Client {
	cfg := config.GetAWSConfig()
	db := awsClient.NewDynamoDBClient(cfg)
	awsClient.CreateTable(config.EventTable)
	awsClient.CreateTable(config.EventLogsTable)
	return db
}

func handleEventLog(ctx context.Context, eventService *service.EventService,
	eventLog *models.EventLog, status string, details string) error {
	eventLog.Status = status
	eventLog.Details = details
	return eventService.LogEventStatus(ctx, eventLog)
}

func handleMessage(ctx context.Context, consumer *event.KafkaConsumer,
	eventService *service.EventService, kafkaMsg *kafka.Message) {
	msg := string(kafkaMsg.Value)
	log.Printf("Received message: %s", msg)

	eventLog := models.EventLog{
		EventMessage: msg,
		Timestamp:    time.Now(),
	}

	var eventMessage models.EventMessage
	if err := json.Unmarshal([]byte(msg), &eventMessage); err != nil {
		handleEventLog(ctx, eventService, &eventLog, "FAILED", err.Error())
		consumer.CommitOffset(kafkaMsg)
		return
	}

	eventLog.EventID = eventMessage.EventID
	eventLog.EventMessage = eventMessage

	if err := eventService.ValidateEventMessage(&eventMessage); err != nil {
		log.Printf("Invalid Event Message: %v", err)
		handleEventLog(ctx, eventService, &eventLog, "FAILED", err.Error())
		consumer.CommitOffset(kafkaMsg)
		return
	}

	if err := eventService.CreateEventMessage(ctx, &eventMessage); err != nil {
		log.Printf("Failed to save event: %v", err)
		handleEventLog(ctx, eventService, &eventLog, "FAILED", err.Error())
		return
	}

	handleEventLog(ctx, eventService, &eventLog, "SAVED", "Event successfully persisted")
	consumer.CommitOffset(kafkaMsg)
}

func main() {
	db := initDynamoDB()
	eventService := service.NewEventService(db)

	msgCH := make(chan *kafka.Message, 64)
	consumer, err := event.NewKafkaConsumer(msgCH)
	if err != nil {
		panic(err)
	}

	for !consumer.IsReady {
		time.Sleep(100 * time.Millisecond)
	}

	log.Println("Listening for messages")

	for kafkaMsg := range msgCH {
		go handleMessage(context.Background(), consumer, eventService, kafkaMsg)
	}
}
