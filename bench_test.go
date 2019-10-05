// +build bench

package worker

import (
	"fmt"
	"testing"
	"time"
)

var goChannelConfig = `
{
	"queues": [
		{
			"name":"queue-1",
			"queue_type":"go_channel",
			"queue_concurrency": 3,
			"worker_concurrency":100,
			"enabled":true,
			"go_channel": {
				"size": 0
			}
		}
	]
}
`

func getMessage(id string) []byte {
	return []byte(fmt.Sprintf(`{"job_id":"test-job-id-%s","job_type":"test-job-type-1","payload":"%s"}`, id, id))
}

// ------------------------------------------------------------------

type Basic struct {
	ID  string `json:"id"`
	Now int64
}

// Test Race condition
func (tj *Basic) Run(j *Job) error       { return nil }
func (tj *Basic) Done(j *Job, err error) {}

func BenchmarkLoops(b *testing.B) {
	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &Basic{} })
	source, _ := h.Queue("queue-1")
	go func() {
		for i := 0; i < b.N; i++ {
			source.Send([]byte("{\"job_id\":\"test-job-id-dd\",\"job_type\":\"test-job-type-1\",\"payload\":\"dd\"}"))
		}
		time.Sleep(100 * time.Millisecond) // Give the worker some time to receive the message.
		h.Shutdown()
	}()
	h.Run()
	b.Logf("counter/total: %d/%d\n", h.JobDoneCounter(), b.N)
}

// ------------------------------------------------------------------
