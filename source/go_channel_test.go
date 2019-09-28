package source

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	c := goChannelConfig{Size: 0}
	assert.Nil(t, c.validate())
}

func TestNew(t *testing.T) {
	c := goChannelConfig{Size: 2}
	s := c.New()
	assert.NotNil(t, s)
	assert.Equal(t, 0, s.(*GoChannel).Len()) // channel is empty
}

func TestSend(t *testing.T) {
	expected := 3
	c := goChannelConfig{Size: int64(expected)}
	s := c.New()
	for i := 0; i < expected; i++ {
		s.Send([]byte("foo"))
	}
	assert.Equal(t, expected, s.(*GoChannel).Len())
}

func TestReceive(t *testing.T) {
	c := goChannelConfig{Size: 1}
	s := c.New()
	s.Send([]byte("a"))
	msg, err := s.Receive()
	assert.Nil(t, err)
	assert.Equal(t, []byte("a"), msg.([]byte))
}
