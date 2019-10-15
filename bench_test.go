// +build bench

package worker

import (
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

type GoChannel struct{}

func (tj *GoChannel) Run(m Messenger) error       { return nil }
func (tj *GoChannel) Done(m Messenger, err error) {}

func BenchmarkGoChannel(b *testing.B) {
	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &GoChannel{} })
	source, _ := h.Queue("queue-1")
	go func() {
		for i := 0; i < b.N; i++ {
			source.Send([]byte("{\"id\":\"test-job-id-dd\",\"type\":\"test-job-type-1\",\"payload\":\"dd\"}"))
		}
		time.Sleep(100 * time.Millisecond) // Give the worker some time to receive the message.
		h.Shutdown()
	}()
	h.Run()
	b.Logf("counter/total: %d/%d\n", h.JobDoneCounter(), b.N)
}
