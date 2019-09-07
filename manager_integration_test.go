// +build integration

package worker

import (
	"fmt"
	"math/rand"
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

// Test register + getJobTypes

func sendMessage(id int) string {
	return fmt.Sprintf(`{"job_id":"test-job-id-%d","job_type":"test-job-type-1","payload":"{\"id\":\"%d\",\"now\":%d}"}`, id, id, time.Now().Unix())
}

func TestWorker(t *testing.T) {
	conf := `{"env":{"env":"dev"},"containers":[{"name":"queue-1","provider":"gochannel","endpoint":"","source":"","concurrency":100,"enabled":true}]}`
	doneCh := make(chan *Job)

	// New manager
	m := New()
	m.SetConfigWithJSON(conf)
	m.SetNotifyChan(doneCh)
	m.Run()

	// Initialise job
	var wg sync.WaitGroup
	var jt = TestJob{Wg: &wg}
	m.InitJobType(jt, "queue-1", "test-job-type-1")

	counter := 0
	total := 50000
	rand.Seed(time.Now().UnixNano())
	go func(wg *sync.WaitGroup, total int) {
		for i := 0; i < total; i++ {
			wg.Add(1)
			go func(i int) {
				Queue <- sendMessage(i)
			}(i)
		}
	}(&wg, total)
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

// Test Race condition
func (tj TestJob) Run(j *Job) {}
