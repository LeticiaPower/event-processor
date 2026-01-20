package config

import (
	"context"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

const (
	EventTable     = "event"
	EventLogsTable = "event_logs"
)

var (
	cfg      aws.Config
	once     sync.Once
	QueueURL string
)

func GetAWSConfig() aws.Config {
	once.Do(func() {
		var err error
		cfg, err = awsConfig.LoadDefaultConfig(context.Background(),
			awsConfig.WithRegion("us-east-1"),
			awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				"test",
				"test",
				"",
			)),
		)
		if err != nil {
			log.Fatalf("error during AWS config: %v", err)
		}
	})
	return cfg
}
