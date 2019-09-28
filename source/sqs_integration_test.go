// +build integration

package source

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
)

func TestSqsSendAndReceive(t *testing.T) {
	c := sqsConfig{
		QueueUrl:            "http://sqs.us-east-1.localhost:4100/100010001000/source-integration-test",
		Region:              "us-east-1",
		UseLocalSqs:         true,
		MaxNumberOfMessages: 10,
	}
	s, err := c.New()
	assert.NotNil(t, s)
	assert.NoError(t, err)

	// Send
	sendRes, err := s.Send([][]byte{[]byte("foo"), []byte("bar")})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(sendRes.(*sqs.SendMessageBatchOutput).Successful))

	// Receive
	recRes, err := s.Receive()
	assert.NoError(t, err)
	msgs := recRes.([][]byte)
	assert.Equal(t, 2, len(msgs))
}

func TestSqsReceiveBehaviours(t *testing.T) {
	tests := []struct {
		maxNumberOfMessages       int64 // maximum number of messages to return each time
		sendBatchNumberOfMessages int   // number of messages sent each time
		expectedNumberOfMessages  []int // expected number of messages received in many times until queue is empty
	}{
		{1, 1, []int{1}},
		{6, 1, []int{1}},
		{6, 3, []int{3}},
		{1, 6, []int{1, 1, 1, 1, 1, 1}},
		{3, 3, []int{3}},
		{3, 5, []int{3, 2}},
		{3, 8, []int{3, 3, 2}},
		{6, 20, []int{6, 6, 6, 2}},
		{10, 12, []int{10, 2}},
		{10, 18, []int{10, 8}}, // maximum number of messages to return can only be within 1 to 10
		{10, 32, []int{10, 10, 10, 2}},
	}
	for _, tt := range tests {
		c := sqsConfig{
			QueueUrl:            "http://sqs.us-east-1.localhost:4100/100010001000/source-integration-test",
			Region:              "us-east-1",
			UseLocalSqs:         true,
			MaxNumberOfMessages: tt.maxNumberOfMessages,
		}
		s, _ := c.New()

		// Send
		for i := 0; i < tt.sendBatchNumberOfMessages; i++ {
			s.Send([][]byte{[]byte("foo")})
		}

		// Receive
		for _, num := range tt.expectedNumberOfMessages {
			msgs, _ := s.Receive()
			assert.Equal(t, num, len(msgs.([][]byte)))
		}
	}
}
