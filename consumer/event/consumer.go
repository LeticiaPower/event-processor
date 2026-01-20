package event

import (
	"context"
	"event-processor/config"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaConsumer struct {
	consumer *kafka.Consumer
	topic    string
	MsgCH    chan *kafka.Message
	IsReady  bool
}

func (c *KafkaConsumer) initializeKafkaTopic(brokers, topicName string) error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	metadata, err := adminClient.GetMetadata(&topicName, false, 5000)
	if err == nil && metadata != nil {
		log.Printf("Topic '%s' already exists", topicName)
		return c.waitForTopicReady(adminClient, topicName)
	}

	log.Printf("Creating topic '%s'...", topicName)
	topicSpec := kafka.TopicSpecification{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	ctx, cancel = context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
	if err != nil {
		return err
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("Failed to create topic: %v", result.Error)
		}
		log.Printf("Topic '%s' created", result.Topic)
	}

	return c.waitForTopicReady(adminClient, topicName)
}

func (c *KafkaConsumer) waitForTopicReady(adminClient *kafka.AdminClient, topicName string) error {
	maxRetries := 10
	retryInterval := 100 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		metadata, err := adminClient.GetMetadata(&topicName, false, 5000)
		cancel()

		if err == nil && metadata != nil && len(metadata.Topics) > 0 {
			topicMetadata := metadata.Topics[topicName]
			if len(topicMetadata.Partitions) > 0 {
				log.Printf("Topic '%s' is ready", topicName)
				c.IsReady = true
				return nil
			}
		}

		log.Printf("Waiting for topic '%s' to be ready", topicName)
		time.Sleep(retryInterval)
	}

	return fmt.Errorf("Topic '%s' did not turn ready", topicName)
}

func (c *KafkaConsumer) ReadMessageLoop() (string, error) {
	for {
		msg, err := c.consumer.ReadMessage(100 * time.Millisecond)
		if err != nil {
			if !err.(kafka.Error).IsTimeout() {
				log.Printf("Consumer error: %v", err)
			}
			continue
		}

		if msg == nil {
			continue
		}

		c.MsgCH <- msg
	}

}

func NewKafkaConsumer(msgCH chan *kafka.Message) (*KafkaConsumer, error) {

	cfg := config.NewKafkaConfig()
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.Host,
		"group.id":           cfg.ConsumerGroup,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"isolation.level":    "read_committed",
	})

	if err != nil {
		return nil, err
	}

	consumer := &KafkaConsumer{
		consumer: c,
		topic:    cfg.Topic,
		MsgCH:    msgCH,
	}
	consumer.initializeKafkaTopic(cfg.Host, cfg.Topic)
	err = c.SubscribeTopics([]string{cfg.Topic}, nil)

	if err != nil {
		return nil, err
	}

	go consumer.ReadMessageLoop()

	return consumer, nil
}

func (c *KafkaConsumer) CommitOffset(msg *kafka.Message) error {
	_, err := c.consumer.CommitMessage(msg)
	return err
}
