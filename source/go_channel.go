package source

type GoChannel struct {
	ch chan []byte
}

func newGoChannel(size int) *GoChannel {
	return &GoChannel{ch: make(chan []byte, size)}
}

func (c *GoChannel) Send(s []byte) error {
	c.ch <- s
	return nil
}

func (c *GoChannel) Receive() ([][]byte, error) {
	return [][]byte{<-c.ch}, nil
}
