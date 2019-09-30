package worker

import (
	"encoding/json"
	"log"
	"time"
)

type fetcher struct {
	worker        *worker
	signalHandler *signalHandler
	stopQueueCh   chan bool
}

func newFetcher() *fetcher {
	return &fetcher{stopQueueCh: make(chan bool)}
}

func (f *fetcher) receive() {
	for {
		// Graceful shutdown
		select {
		case <-f.stopQueueCh:
			log.Println("Stop receiving message and start waiting for the rest of jobs to be done")
			return
		default:
		}

		message, err := f.worker.queue.Receive()
		if err != nil {
			log.Println("Error: ", err)
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
					log.Printf("Error: %s, message: %s\n", err, string(msg))
				}
			}
		case []byte:
			if len(message.([]byte)) == 0 {
				continue
			}
			var j Job
			if err = f.processMessage(message.([]byte), &j); err != nil {
				log.Printf("Error: %s, message: %s\n", err, string(message.([]byte)))
			}
		default:
			log.Println("Error: unknown type of return from Receive()")
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
		log.Printf("Job type '%s'.'%s' not found\n", f.worker.config.Name, j.Desc.JobType)
		return
	}
	j.receivedAt = time.Now()
	f.signalHandler.wg.Add(1)
	f.worker.receivedChan <- j
	return
}