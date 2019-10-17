package worker

import (
	"encoding/json"
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
				var msg message
				if err = f.dispatch(payload, &msg); err != nil {
					f.logger.Printf("Error: %s, message: %s\n", err, string(payload))
				}
			}
		case []byte:
			if len(text.([]byte)) == 0 {
				continue
			}
			var msg message
			if err = f.dispatch(text.([]byte), &msg); err != nil {
				f.logger.Printf("Error: %s, message: %s\n", err, string(text.([]byte)))
			}
		default:
			f.logger.Println("Error: unknown type of return from Receive()")
			continue
		}
	}
}

func (f *fetcher) dispatch(payload []byte, msg *message) (err error) {
	if err = json.Unmarshal(payload, &msg.descriptor); err != nil {
		return
	}
	if err = msg.validate(); err != nil {
		return
	}
	if _, ok := f.worker.jobTypes[msg.descriptor.Type]; !ok {
		f.logger.Printf("Job type '%s'.'%s' not found\n", f.worker.config.Name, msg.descriptor.Type)
		return
	}
	msg.receivedAt = time.Now()
	f.signalHandler.wg.Add(1)
	f.worker.undoneMessageCh <- msg
	return
}
