package worker

import (
	"fmt"
	"log"
	"time"
	"worker/queue"
)

type fetcher struct {
	config        *queue.Config
	logger        *log.Logger
	stopQueueCh   chan bool
	signalHandler *signalHandler
	worker        *worker
}

func newFetcher() *fetcher {
	return &fetcher{stopQueueCh: make(chan bool)}
}

func (f *fetcher) poll(i int64) {
	for {
		// Graceful shutdown
		select {
		case <-f.stopQueueCh:
			f.logger.Printf("Closing %s[%d]\n", f.config.Name, i)
			return
		default:
		}

		text, err := f.worker.queue.Receive()
		if err != nil {
			f.logger.Println("Error: ", err)
			continue
		}

		// Check the type of return from Receive()
		switch text.(type) {
		case [][]byte:
			if len(text.([][]byte)) == 0 {
				continue
			}
			for _, payload := range text.([][]byte) {
				if err = f.dispatch(payload); err != nil {
					f.logger.Printf("Error: %s, Message: %s\n", err, string(payload))
				}
			}
		case []byte:
			if len(text.([]byte)) == 0 {
				continue
			}
			if err = f.dispatch(text.([]byte)); err != nil {
				f.logger.Printf("Error: %s, message: %s\n", err, string(text.([]byte)))
			}
		default:
			f.logger.Println("Error: unknown type of return from Receive()")
			continue
		}
	}
}

func (f *fetcher) dispatch(payload []byte) (err error) {
	msg, err := newMessage(payload)
	if err != nil {
		return
	}
	if err = f.validate(&msg); err != nil {
		return
	}
	msg.queueName = f.config.Name
	msg.receivedAt = time.Now()
	f.signalHandler.wg.Add(1)
	f.worker.undoneMessageCh <- &msg
	return
}

func (f *fetcher) validate(msg *Message) error {
	// Check if job type has already registered
	if _, ok := f.worker.jobTypes[msg.descriptor.Type]; !ok {
		return fmt.Errorf("Job type '%s'.'%s' not registered\n", f.config.Name, msg.descriptor.Type)
	}
	return nil
}
