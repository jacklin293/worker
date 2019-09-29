// +build unit

package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSqsValidate(t *testing.T) {
	tests := []struct {
		queueUrl            string
		region              string
		maxNumberOfMessages int64
		hasErr              bool
	}{
		{"", "", 0, true},
		{"http://sqs.us-east-1.localhost:4100/100010001000/test-queue", "", 0, true},
		{"http://sqs.us-east-1.localhost:4100/100010001000/test-queue", "us-east-1", -1, true},
		{"http://sqs.us-east-1.localhost:4100/100010001000/test-queue", "us-east-1", 0, false},
		{"http://sqs.us-east-1.localhost:4100/100010001000/test-queue", "us-east-1", 1, false},
		{"http://sqs.us-east-1.localhost:4100/100010001000/test-queue", "us-east-1", 5, false},
		{"http://sqs.us-east-1.localhost:4100/100010001000/test-queue", "us-east-1", 9, false},
		{"http://sqs.us-east-1.localhost:4100/100010001000/test-queue", "us-east-1", 10, false},
		{"http://sqs.us-east-1.localhost:4100/100010001000/test-queue", "us-east-1", 11, true},
	}
	for _, tt := range tests {
		c := sqsConfig{QueueUrl: tt.queueUrl, Region: tt.region, MaxNumberOfMessages: tt.maxNumberOfMessages}
		if tt.hasErr {
			assert.NotNil(t, c.validate())
		} else {
			assert.NoError(t, c.validate())
		}
	}
}

func TestSqsNew(t *testing.T) {
	tests := []struct {
		queueUrl            string
		useLocalSqs         bool
		maxNumberOfMessages int64
		visibilityTimeout   int64
		waitTimeSeconds     int64
	}{
		{"http://sqs.us-east-1.localhost:4100/100010001000/test-queue", true, 1, 1, 0},
		{"http://sqs.us-east-1.localhost:4100/100010001000/test-queue", true, 1, 1, 1},
	}
	for _, tt := range tests {
		c := sqsConfig{
			QueueUrl:            tt.queueUrl,
			UseLocalSqs:         tt.useLocalSqs,
			Region:              "us-east-1",
			MaxNumberOfMessages: tt.maxNumberOfMessages,
			VisibilityTimeout:   tt.visibilityTimeout,
			WaitTimeSeconds:     tt.waitTimeSeconds,
		}
		s, err := c.New()
		assert.NotNil(t, s)
		assert.NoError(t, err)
	}
}

func TestGetEndpoint(t *testing.T) {
	tests := []struct {
		queueUrl string
		expected string
	}{
		{"http://sqs.us-east-1.localhost:4100/100010001000/test-queue", "http://sqs.us-east-1.localhost:4100"},
		{"https://sqs.us-east-1.localhost:4100/100010001000/test-queue", "https://sqs.us-east-1.localhost:4100"},
		{"http://sqs.us-east-1.localhost/100010001000/test-queue", "http://sqs.us-east-1.localhost"},
	}
	for _, tt := range tests {
		c := sqsConfig{QueueUrl: tt.queueUrl}
		result, _ := c.getEndpoint()
		assert.Equal(t, tt.expected, result)
	}
}
