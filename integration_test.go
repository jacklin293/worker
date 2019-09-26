// +build integration

package worker

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestJob struct {
	Wg  *sync.WaitGroup
	ID  string `json:"id"`
	Now int64
}

var singleTopicConfig = `[
	{
		"name":"queue-1",
		"source_type":"go_channel",
		"source_concurrency": 3,
		"worker_concurrency":100,
		"enabled":true,
		"go_channel": {
			"size": 0
		}
	}
]`

// Test Race condition
func (tj TestJob) Run(j *Job) {
	time.Sleep(1 * time.Millisecond)
}

func TestWorker(t *testing.T) {
	doneCh := make(chan *Job)

	// New handler
	m := New()
	m.SetConfigWithJSON(singleTopicConfig)
	m.SetNotifyChan(doneCh)
	m.Run()
	source, _ := m.GetSourceByName("queue-1")

	// Initialise job
	var wg sync.WaitGroup
	var jt = TestJob{}
	m.RegisterJobType(jt, "queue-1", "test-job-type-1")

	counter := 0
	total := 50000
	go func(wg *sync.WaitGroup, total int) {
		for i := 0; i < total; i++ {
			wg.Add(1)
			go func(i int) {
				source.Send(sendMessage(i))
			}(i)
		}
	}(&wg, total)
	// Let wg.Add work first in order to prevent testing from finishing too soon
	time.Sleep(1 * time.Millisecond)

	go func(wg *sync.WaitGroup, counter *int) {
		for {
			select {
			case <-doneCh:
				*counter++
				wg.Done()
			}
		}
	}(&wg, &counter)
	wg.Wait()
	t.Log("counter:", counter)
	assert.Equal(t, total, counter)
}

func BenchmarkWorker(b *testing.B) {
	doneCh := make(chan *Job)

	// New handler
	m := New()
	m.SetConfigWithJSON(singleTopicConfig)
	m.SetNotifyChan(doneCh)
	m.Run()
	source, _ := m.GetSourceByName("queue-1")

	// Initialise job
	var wg sync.WaitGroup
	var jt = TestJob{}
	m.RegisterJobType(jt, "queue-1", "test-job-type-1")

	total := b.N
	go func(wg *sync.WaitGroup, total int) {
		for i := 0; i < total; i++ {
			wg.Add(1)
			go func(i int) {
				source.Send(sendMessage(i))
			}(i)
		}
	}(&wg, total)
	time.Sleep(1 * time.Millisecond)

	for i := 0; i < b.N; i++ {
		select {
		case <-doneCh:
			wg.Done()
		}
	}
}

func BenchmarkWorker1kJobs(b *testing.B) {
	doneCh := make(chan *Job)

	// New handler
	m := New()
	m.SetConfigWithJSON(singleTopicConfig)
	m.SetNotifyChan(doneCh)
	m.Run()
	source, _ := m.GetSourceByName("queue-1")

	// Initialise job
	var wg sync.WaitGroup
	var jt = TestJob{}
	m.RegisterJobType(jt, "queue-1", "test-job-type-1")

	for i := 0; i <= b.N; i++ {
		total := 1000
		go func(wg *sync.WaitGroup, total int) {
			for i := 0; i < total; i++ {
				wg.Add(1)
				go func(i int) {
					source.Send(sendMessage(i))
				}(i)
			}
		}(&wg, total)
		time.Sleep(1 * time.Millisecond)

		go func(wg *sync.WaitGroup) {
			for {
				select {
				case <-doneCh:
					wg.Done()
				}
			}
		}(&wg)
		wg.Wait()
	}
}

func sendMessage(id int) []byte {
	return []byte(fmt.Sprintf(`{"job_id":"test-job-id-%d","job_type":"test-job-type-1","payload":"{\"id\":\"%d\",\"now\":%d}"}`, id, id, time.Now().Unix()))
}
