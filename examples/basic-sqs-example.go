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
			"queue_type":"sqs",
			"queue_concurrency":2,
			"worker_concurrency": 2,
			"enabled":true,
			"sqs":{
				"queue_url":"http://localhost:4100/100010001000/integration-test",
				"use_local_sqs": true,
				"region":"us-east-1",
				"max_number_of_messages": 1,
				"wait_time_seconds": 5
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
			q.Send([][]byte{[]byte(fmt.Sprintf(`{"id":"id-%d","type":"test-job-type-1","payload":"%d"}`, i, i))})
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
