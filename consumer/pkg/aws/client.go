package aws

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

var DynamoDBClient *dynamodb.Client

func NewDynamoDBClient(cfg aws.Config) *dynamodb.Client {
	localstackEndpoint := "http://127.0.0.1:4566"

	DynamoDBClient = dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(localstackEndpoint)
		o.EndpointOptions.DisableHTTPS = true
	})
	return DynamoDBClient
}
