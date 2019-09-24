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

func (c *goChannelConfig) new() Sourcer {
	return &GoChannel{
		ch:     make(chan []byte, c.Size),
		config: c,
	}
}

func (c *GoChannel) Send(s []byte) error {
	c.ch <- s
	return nil
}

func (c *GoChannel) Receive() ([][]byte, error) {
	return [][]byte{<-c.ch}, nil
}
