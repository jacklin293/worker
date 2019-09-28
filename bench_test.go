// +build bench

package worker

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

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

var signFn = func() Contract {
	return &TestJob{}
}

type TestJob struct {
	ID  string `json:"id"`
	Now int64
}

// Test Race condition
func (tj *TestJob) Run(j *Job) error {
	time.Sleep(1 * time.Millisecond)
	return nil
}
func (tj *TestJob) Done(j *Job, err error) {}

func getMessage(id string) []byte {
	return []byte(fmt.Sprintf(`{"job_id":"test-job-id-%s","job_type":"test-job-type-1","payload":"{\"id\":\"%s\",\"timestamp\":%d}"}`, id, id, time.Now().Unix()))
}

func BenchmarkLoops(b *testing.B) {
	doneCh := make(chan *Job)

	// New handler
	m := New()
	m.SetConfigWithJSON(singleTopicConfig)
	m.SetNotifyChan(doneCh)
	m.Run()
	source, _ := m.GetSourceByName("queue-1")

	// Initialise job
	var wg sync.WaitGroup
	m.RegisterJobType("queue-1", "test-job-type-1", signFn)

	total := b.N
	go func(wg *sync.WaitGroup, total int) {
		for i := 0; i < total; i++ {
			wg.Add(1)
			go func(i int) {
				source.Send(getMessage("foo"))
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

func BenchmarkLoops1kJobs(b *testing.B) {
	doneCh := make(chan *Job)

	// New handler
	m := New()
	m.SetConfigWithJSON(singleTopicConfig)
	m.SetNotifyChan(doneCh)
	m.Run()
	source, _ := m.GetSourceByName("queue-1")

	// Initialise job
	var wg sync.WaitGroup
	m.RegisterJobType("queue-1", "test-job-type-1", signFn)

	for i := 0; i <= b.N; i++ {
		total := 1000
		go func(wg *sync.WaitGroup, total int) {
			for i := 0; i < total; i++ {
				wg.Add(1)
				go func(i int) {
					source.Send(getMessage("foo"))
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
