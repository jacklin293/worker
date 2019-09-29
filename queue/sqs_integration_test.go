// +build integration

package queue

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
)

func TestSqsBasicSendAndReceive(t *testing.T) {
	c := sqsConfig{
		QueueUrl:    "http://localhost:4100/100010001000/queue-integration-test",
		UseLocalSqs: true,
		Region:      "us-east-1",
	}
	s, err := c.New()
	assert.NotNil(t, s)
	if !assert.NoError(t, err) {
		return
	}

	// Send
	sendRes, err := s.Send([][]byte{[]byte("foo")})
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, 1, len(sendRes.(*sqs.SendMessageBatchOutput).Successful))

	// Receive
	recRes, err := s.Receive()
	if !assert.NoError(t, err) {
		return
	}
	msgs := recRes.([][]byte)
	if assert.Equal(t, 1, len(msgs)) {
		assert.Equal(t, "foo", string(msgs[0]))
	}
}

func TestSqsMessagesOfSendAndReceive(t *testing.T) {
	c := sqsConfig{
		QueueUrl:            "http://localhost:4100/100010001000/queue-integration-test",
		UseLocalSqs:         true,
		Region:              "us-east-1",
		MaxNumberOfMessages: 10,
	}
	s, err := c.New()
	assert.NotNil(t, s)
	if !assert.NoError(t, err) {
		return
	}

	// Send
	sendRes, err := s.Send([][]byte{[]byte("foo"), []byte("bar")})
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, 2, len(sendRes.(*sqs.SendMessageBatchOutput).Successful))

	// Receive
	recRes, err := s.Receive()
	if !assert.NoError(t, err) {
		return
	}
	msgs := recRes.([][]byte)

	// Messages might be retrived in random order
	table := map[string]bool{"foo": false, "bar": false}
	assert.Equal(t, len(table), len(msgs))
	for _, msg := range msgs {
		key := string(msg)
		_, ok := table[key]
		assert.True(t, ok)
		assert.False(t, table[key]) // make sure every item is unique
		table[key] = true           // flag the item
	}
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
			QueueUrl:            "http://localhost:4100/100010001000/queue-integration-test",
			UseLocalSqs:         true,
			Region:              "us-east-1",
			MaxNumberOfMessages: tt.maxNumberOfMessages,
		}
		s, err := c.New()
		assert.NotNil(t, s)
		if !assert.NoError(t, err) {
			return
		}

		// Send
		for i := 0; i < tt.sendBatchNumberOfMessages; i++ {
			_, err = s.Send([][]byte{[]byte("foo")})
			if !assert.NoError(t, err) {
				return
			}
		}

		// Receive
		for _, num := range tt.expectedNumberOfMessages {
			msgs, err := s.Receive()
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, num, len(msgs.([][]byte)))
		}
	}
}
