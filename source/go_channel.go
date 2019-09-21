package source

type GoChannel struct {
	receivedCh chan []byte
}

func (c *GoChannel) Send(msg []byte) error {
	c.receivedCh <- msg
	return nil
}

func (c *GoChannel) Receive() ([]byte, error) {
	return <-c.receivedCh, nil
}
