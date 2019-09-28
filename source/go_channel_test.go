// +build unit

package source

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGoChannelValidate(t *testing.T) {
	tests := []struct {
		size   int64
		hasErr bool
	}{
		{-1, true},
		{0, false},
		{1, false},
	}
	for _, tt := range tests {
		c := goChannelConfig{Size: tt.size}
		if tt.hasErr {
			assert.NotNil(t, c.validate())
		} else {
			assert.NoError(t, c.validate())
		}
	}
}

func TestGoChannelNew(t *testing.T) {
	c := goChannelConfig{Size: 2}
	s, err := c.New()
	assert.NotNil(t, s)
	assert.NoError(t, err)
	assert.Equal(t, 0, s.(*GoChannel).Len()) // channel is empty
}

func TestGoChannelSend(t *testing.T) {
	expected := 3
	c := goChannelConfig{Size: int64(expected)}
	s, err := c.New()
	assert.NoError(t, err)
	for i := 0; i < expected; i++ {
		s.Send([]byte("foo"))
	}
	assert.Equal(t, expected, s.(*GoChannel).Len())
}

func TestGoChannelReceive(t *testing.T) {
	c := goChannelConfig{Size: 1}
	s, err := c.New()
	assert.NoError(t, err)
	s.Send([]byte("a"))
	msg, err := s.Receive()
	assert.NoError(t, err)
	assert.Equal(t, []byte("a"), msg.([]byte))
}
