package config

type KafkaConfig struct {
	Topic         string
	ConsumerGroup string
	Host          string
}

func NewKafkaConfig() *KafkaConfig {
	return &KafkaConfig{
		Topic:         "pismo_topic",
		ConsumerGroup: "pismo_cg",
		Host:          "127.0.0.1:9092",
	}
}
