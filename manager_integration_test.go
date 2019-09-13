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

// Test Race condition
func (tj TestJob) Run(j *Job) {}

func TestWorker(t *testing.T) {
	conf := `{"env":{"env":"dev"},"containers":[{"name":"queue-1","provider":"gochannel","endpoint":"","source":"","concurrency":100,"enabled":true}]}`
	doneCh := make(chan *Job)
	receiveCh := make(chan []byte)

	// New manager
	m := New()
	m.SetConfigWithJSON(conf)
	m.SetNotifyChan(doneCh)
	m.SetReceiveChan(receiveCh)
	m.Run()

	// Initialise job
	var wg sync.WaitGroup
	var jt = TestJob{}
	m.InitJobType(jt, "queue-1", "test-job-type-1")

	counter := 0
	total := 50000
	go func(wg *sync.WaitGroup, total int) {
		for i := 0; i < total; i++ {
			wg.Add(1)
			go func(i int) {
				receiveCh <- sendMessage(i)
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
	conf := `{"env":{"env":"dev"},"containers":[{"name":"queue-1","provider":"gochannel","endpoint":"","source":"","concurrency":100,"enabled":true}]}`
	doneCh := make(chan *Job)
	receiveCh := make(chan []byte)

	// New manager
	m := New()
	m.SetConfigWithJSON(conf)
	m.SetNotifyChan(doneCh)
	m.SetReceiveChan(receiveCh)
	m.Run()

	// Initialise job
	var wg sync.WaitGroup
	var jt = TestJob{}
	m.InitJobType(jt, "queue-1", "test-job-type-1")

	total := b.N
	go func(wg *sync.WaitGroup, total int) {
		for i := 0; i < total; i++ {
			wg.Add(1)
			go func(i int) {
				receiveCh <- sendMessage(i)
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

func BenchmarkWorker10kJobs(b *testing.B) {
	conf := `{"env":{"env":"dev"},"containers":[{"name":"queue-1","provider":"gochannel","endpoint":"","source":"","concurrency":100,"enabled":true}]}`
	doneCh := make(chan *Job)
	receiveCh := make(chan []byte)

	// New manager
	m := New()
	m.SetConfigWithJSON(conf)
	m.SetNotifyChan(doneCh)
	m.SetReceiveChan(receiveCh)
	m.Run()

	// Initialise job
	var wg sync.WaitGroup
	var jt = TestJob{}
	m.InitJobType(jt, "queue-1", "test-job-type-1")

	for i := 0; i <= b.N; i++ {
		total := 10000
		go func(wg *sync.WaitGroup, total int) {
			for i := 0; i < total; i++ {
				wg.Add(1)
				go func(i int) {
					receiveCh <- sendMessage(i)
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
