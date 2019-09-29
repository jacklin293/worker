// +build integration

package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGoChannelBasicSendAndReceive(t *testing.T) {
	c := goChannelConfig{Size: 0}
	s, err := c.New()
	assert.NotNil(t, s)
	assert.NoError(t, err)

	// Send
	msgs := [][]byte{[]byte("foo"), []byte("bar"), []byte("baz")}
	go func() {
		for _, msg := range msgs {
			_, err = s.Send(msg)
			assert.NoError(t, err)
		}
	}()

	// Receive
	for _, msg := range msgs {
		recMsg, err := s.Receive()
		assert.NoError(t, err)
		assert.Equal(t, msg, recMsg.([]byte))
	}
}

func TestGoChannelLen(t *testing.T) {
	total := []int{3, 10, 100}
	for _, count := range total {
		c := goChannelConfig{Size: int64(count)}
		s, _ := c.New()

		// Send
		for i := 0; i < count; i++ {
			s.Send([]byte("foo"))
		}
		assert.Equal(t, count, s.(*GoChannel).Len())

		// Receive
		for i := 0; i < count; i++ {
			s.Receive()
		}
	}
}
