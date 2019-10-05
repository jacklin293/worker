package worker

import (
	"encoding/json"
	"log"
	"time"
	"worker/queue"
)

type fetcher struct {
	config        *queue.Config
	worker        *worker
	signalHandler *signalHandler
	stopQueueCh   chan bool
	logger        *log.Logger
}

func newFetcher() *fetcher {
	return &fetcher{stopQueueCh: make(chan bool)}
}

func (f *fetcher) receive(i int64) {
	for {
		// Graceful shutdown
		select {
		case <-f.stopQueueCh:
			f.logger.Printf("Closing %s[%d]\n", f.config.Name, i)
			return
		default:
		}

		message, err := f.worker.queue.Receive()
		if err != nil {
			f.logger.Println("Error: ", err)
			continue
		}

		// Check the type of return from Receive()
		switch message.(type) {
		case [][]byte:
			if len(message.([][]byte)) == 0 {
				continue
			}
			for _, msg := range message.([][]byte) {
				var j Job
				if err = f.processMessage(msg, &j); err != nil {
					f.logger.Printf("Error: %s, message: %s\n", err, string(msg))
				}
			}
		case []byte:
			if len(message.([]byte)) == 0 {
				continue
			}
			var j Job
			if err = f.processMessage(message.([]byte), &j); err != nil {
				f.logger.Printf("Error: %s, message: %s\n", err, string(message.([]byte)))
			}
		default:
			f.logger.Println("Error: unknown type of return from Receive()")
			continue
		}
	}
}

func (f *fetcher) processMessage(msg []byte, j *Job) (err error) {
	if err = json.Unmarshal(msg, &j.Desc); err != nil {
		return
	}
	if err = j.validate(); err != nil {
		return
	}
	if _, ok := f.worker.jobTypes[j.Desc.JobType]; !ok {
		f.logger.Printf("Job type '%s'.'%s' not found\n", f.worker.config.Name, j.Desc.JobType)
		return
	}
	j.receivedAt = time.Now()
	f.signalHandler.wg.Add(1)
	f.worker.receivedChan <- j
	return
}
