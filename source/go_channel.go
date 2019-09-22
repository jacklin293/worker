package source

type GoChannel struct {
	receivedCh chan []byte
}

func newGoChannel() *GoChannel {
	return &GoChannel{receivedCh: make(chan []byte)}
}

func (c *GoChannel) Send(msg []byte) error {
	c.receivedCh <- msg
	return nil
}

func (c *GoChannel) Receive() ([][]byte, error) {
	return [][]byte{<-c.receivedCh}, nil
}
