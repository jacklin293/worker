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

type DB interface {
	GetConn() string
}

type MySQL struct{}

func (m *MySQL) GetConn() string {
	return "Conn 1"
}

// Implementation of job type
type TestJob struct {
	mysql DB
}

func (tj *TestJob) Do(m worker.Messenger) error {
	fmt.Println("Processing job", m.Payload())
	fmt.Println("Conn: ", tj.mysql.GetConn())
	return nil
}
func (tj *TestJob) Done(m worker.Messenger, err error) {}

func main() {
	var mysql *MySQL

	// New worker
	h := worker.New()
	h.SetConfig(conf)

	// Register job type
	h.RegisterJobType("queue-1", "test-job-type-1", func() worker.Job {
		return &TestJob{mysql: mysql}
	})

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
