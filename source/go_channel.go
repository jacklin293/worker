package source

// Config
type goChannelConfig struct {
	Size int64 `json:"size"`
}

// Implementation
type GoChannel struct {
	ch     chan []byte
	config *goChannelConfig
}

func (c *goChannelConfig) validate() error {
	return nil
}

func (c *goChannelConfig) New() Sourcer {
	return &GoChannel{
		ch:     make(chan []byte, c.Size),
		config: c,
	}
}

func (ch *GoChannel) Send(msg interface{}) (interface{}, error) {
	ch.ch <- msg.([]byte)
	return nil, nil
}

func (ch *GoChannel) Receive() (interface{}, error) {
	return <-ch.ch, nil
}

func (ch *GoChannel) Len() int {
	return len(ch.ch)
}
