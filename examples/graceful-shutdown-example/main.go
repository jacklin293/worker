package main

import (
	"fmt"
	"log"
	"time"
	"worker"
)

// Multiple queues
var conf = `
{
	"log_enabled": true,
	"queues": [
		{
			"name":"queue-1",
			"queue_type":"go_channel",
			"queue_concurrency": 1,
			"worker_concurrency":4,
			"enabled":true,
			"go_channel": {
				"size": 0
			}
		},
		{
			"name":"queue-2",
			"queue_type":"go_channel",
			"queue_concurrency": 2,
			"worker_concurrency":3,
			"enabled":true,
			"go_channel": {
				"size": 0
			}
		}
	]
}`

// Implementation of job type
type TestJob struct{}

func (tj *TestJob) Do(m worker.Messenger) error {
	time.Sleep(3 * time.Second)
	return nil
}
func (tj *TestJob) Done(m worker.Messenger, err error) {}

func main() {
	// New worker
	h := worker.New()
	h.SetConfig(conf)

	// Register job types
	h.RegisterJobType("queue-1", "test-job-type-1", func() worker.Job { return &TestJob{} })
	h.RegisterJobType("queue-1", "test-job-type-2", func() worker.Job { return &TestJob{} })
	h.RegisterJobType("queue-3", "test-job-type-3", func() worker.Job { return &TestJob{} })

	// (for demo) Get the queue for enqueuing

	total := int64(10)
	go func(total int64) {
		go func() {
			// (for demo) Report job status every second
			for {
				time.Sleep(1 * time.Second)
				log.Println("Current job done counter: ", h.JobDoneCounter())
				report(h)
				log.Println("-------------------------------------------------")
			}
		}()
		// (for demo) Enqueue jobs
		q, _ := h.Queue("queue-1")
		for i := int64(0); i < total; i++ {
			go q.Send([]byte(fmt.Sprintf(`{"id":"id-%d","type":"test-job-type-1","payload":"%d"}`, i, i)))
		}

		// (for demo) Wait for a bit
		time.Sleep(300 * time.Millisecond)

		// (for demo) Close this worker
		h.Shutdown()
	}(total)

	// (for demo) Let worker hang here to process jobs
	h.Run()
	log.Println("Job done counter: ", h.JobDoneCounter())
}

func report(h *worker.Handler) {
	log.Print("Job status: ")
	for qName, ws := range h.WorkerStatus() {
		for i, m := range ws {
			if m == nil {
				log.Printf("  %s[%d]: (idle)\n", qName, i)
			} else {
				log.Printf("  %s[%d]: processing %s\n", qName, i, m.Id())
			}
		}
	}
}
