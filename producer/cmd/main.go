package main

import (
	"encoding/json"
	"event-processor/config"
	"log"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaProducer struct {
	producer *kafka.Producer
	topic    string
}

func newProducer() (*KafkaProducer, error) {

	cfg := config.NewKafkaConfig()

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": cfg.Host})
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{
		producer: p,
		topic:    cfg.Topic,
	}, nil
}

func (p *KafkaProducer) send(msg string) {
	log.Println("Sending messages")

	p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          []byte(msg),
	}, nil)

	go func() {
		for e := range p.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivered failed: %v", ev.TopicPartition.Error)
				} else {
					log.Printf("Delivered message to %v", p.topic)
				}
			}
		}
	}()

	remainingMsg := p.producer.Flush(10000)
	if remainingMsg > 0 {
		log.Printf("Warning: %d messages were not delivered\n", remainingMsg)
	}
}

func (p *KafkaProducer) Close() {
	p.producer.Close()
}

func main() {
	log.Println("Producing")

	messages := []string{}

	ts := time.Now().Format(time.RFC3339)

	if len(os.Args) < 2 {
		id := uuid.New().String()

		eventMessage := map[string]interface{}{
			"event_id":    id,
			"event_type":  "event-processor-producer",
			"client_key":  "pismo_client",
			"destination": "client_01",
			"timestamp":   ts,
			"message": map[string]interface{}{
				"text": "This is a message from producer!",
			},
		}
		messageJSON, _ := json.Marshal(eventMessage)
		messages = append(messages, string(messageJSON))
	} else {
		for _, arg := range os.Args[1:] {
			messages = append(messages, arg)
		}
	}

	producer, err := newProducer()

	if err != nil {
		panic(err)
	}
	defer producer.Close()

	time.Sleep(1 * time.Second)

	for _, msg := range messages {
		producer.send(msg)
	}
}
