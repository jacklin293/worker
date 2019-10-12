package main

import (
	"fmt"
	"log"
	"time"
	"worker"
)

// One queue
var conf = `
{
	"log_enabled": true,
	"queues": [
		{
			"name":"queue-1",
			"queue_type":"go_channel",
			"queue_concurrency": 3,
			"worker_concurrency":4,
			"enabled":true,
			"go_channel": {
				"size": 0
			}
		}
	]
}`

// Implementation of job type
type TestJob struct{}

func (tj *TestJob) Run(j *worker.Job) error {
	fmt.Println("Processing job", j.Payload())
	return nil
}
func (tj *TestJob) Done(j *worker.Job, err error) {}

func main() {
	// New worker
	h := worker.New()
	h.InitWithJsonConfig(conf)

	// Register job type
	h.RegisterJobType("queue-1", "test-job-type-1", func() worker.Process { return &TestJob{} })

	total := int64(10)
	go func(total int64) {
		// (for demo) Enqueue jobs
		q, _ := h.Queue("queue-1")
		for i := int64(0); i < total; i++ {
			q.Send([]byte(fmt.Sprintf(`{"id":"id-%d","type":"test-job-type-1","payload":"%d"}`, i, i)))
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
