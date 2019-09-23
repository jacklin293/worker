package source

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewGoChannel(t *testing.T) {
	c := newGoChannel(0)
	assert.NotNil(t, c)
}

func TestSend(t *testing.T) {
	c := newGoChannel(3)
	c.Send([]byte("a"))
	c.Send([]byte("b"))
	c.Send([]byte("c"))
	assert.Equal(t, 3, len(c.ch))
}

func TestReceive(t *testing.T) {
	c := newGoChannel(1)
	c.Send([]byte("a"))
	s, err := c.Receive()
	assert.Nil(t, err)
	assert.Equal(t, []byte("a"), s[0])
}
