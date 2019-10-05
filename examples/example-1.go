package main

import (
	"fmt"
	"log"
	"strconv"
	"time"
	"worker"
)

var conf = `
{
	"shutdown_timeout": 2,
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
		},
		{
			"name":"queue-2",
			"queue_type":"go_channel",
			"queue_concurrency": 4,
			"worker_concurrency":100,
			"enabled":false,
			"go_channel": {
				"size": 0
			}
		},
		{
			"name":"queue-3",
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

type TestJob struct{}

func (tj *TestJob) Run(j *worker.Job) error {
	time.Sleep(5 * time.Second)
	return nil
}
func (tj *TestJob) Done(j *worker.Job, err error) {}

// Test register + getJobTypes
func main() {
	// New manager
	h := worker.New()
	h.InitWithJsonConfig(conf)
	h.RegisterJobType("queue-1", "test-job-type-1", func() worker.Process { return &TestJob{} })
	h.RegisterJobType("queue-1", "test-job-type-2", func() worker.Process { return &TestJob{} })
	h.RegisterJobType("queue-3", "test-job-type-3", func() worker.Process { return &TestJob{} })
	source, _ := h.Queue("queue-1")

	total := int64(20)
	go func(total int64) {
		go func() {
			for {
				time.Sleep(1 * time.Second)
				log.Println("job done counter: ", h.JobDoneCounter())
				// report(h)
				fmt.Print("\n\n\n")
			}
		}()
		for i := int64(0); i < total; i++ {
			source.Send(getMessage(strconv.FormatInt(i, 10)))
		}
	}(total)
	h.Run()
	log.Println("job done counter: ", h.JobDoneCounter())
}

func getMessage(id string) []byte {
	return []byte(fmt.Sprintf(`{"id":"test-job-id-%s","type":"test-job-type-1","payload":"%s"}`, id, id))
}

func report(h *worker.Handler) {
	for qName, ws := range h.WorkerStatus() {
		for i, j := range ws {
			if j == nil {
				fmt.Printf("%s[%d]: (idle)\n", qName, i)
			} else {
				fmt.Printf("%s[%d]: processing %s\n", qName, i, j.Id())
			}
		}
	}
}
