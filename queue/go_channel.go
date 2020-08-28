package queue

import "errors"

// Config
type goChannelConfig struct {
	Size int64 `json:"size"`
}

type GoChannel struct {
	ch     chan []byte
	config *goChannelConfig
}

func (c *goChannelConfig) validate() error {
	if c.Size < 0 {
		return errors.New("size must be greater than or equal to 0")
	}
	return nil
}

func (c *goChannelConfig) New() (QueueContainer, error) {
	return &GoChannel{
		ch:     make(chan []byte, c.Size),
		config: c,
	}, nil
}

func (ch *GoChannel) Send(msg interface{}) (interface{}, error) {
	ch.ch <- msg.([]byte)
	return nil, nil
}

func (ch *GoChannel) Receive() (interface{}, error) {
	return <-ch.ch, nil
}

func (ch *GoChannel) Delete(interface{}) (interface{}, error) {
	return true, nil
}

func (ch *GoChannel) Len() int {
	return len(ch.ch)
}
