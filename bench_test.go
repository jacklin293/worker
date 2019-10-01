// +build bench

package worker

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

var goChannelConfig = `[
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
]`

type TestBasic struct {
	ID  string `json:"id"`
	Now int64
}

// Test Race condition
func (tj *TestBasic) Run(j *Job) error       { return nil }
func (tj *TestBasic) Done(j *Job, err error) {}

func BenchmarkLoops(b *testing.B) {
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &TestBasic{} })
	source, _ := m.GetQueueByName("queue-1")
	go m.Run()

	num := 1000
	counter := num
	for i := 0; i < b.N; i++ {
		go func() {
			for i := 0; i < num; i++ {
				source.Send(getMessage(strconv.Itoa(i)))
			}
		}()
		for {
			time.Sleep(10 * time.Millisecond)
			if m.JobCounter() == int64(counter) {
				counter += num
				break
			}
		}
	}
}

func BenchmarkLoops1kJobs(b *testing.B) {
	b.Skip()
	/*
		doneCh := make(chan *Job)

		// New handler
		m := New()
		m.SetConfigWithJSON(singleTopicConfig)
		m.RegisterJobType("queue-1", "test-job-type-1", signFn)
		m.Run()
		source, _ := m.GetSourceByName("queue-1")

		for i := 0; i <= b.N; i++ {
			total := 1000
			go func(wg *sync.WaitGroup, total int) {
				for i := 0; i < total; i++ {
					wg.Add(1)
					go func(i int) {
						source.Send(getMessage(tj, "foo"))
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
	*/
}

func getMessage(id string) []byte {
	return []byte(fmt.Sprintf(`{"job_id":"test-job-id-%s","job_type":"test-job-type-1","payload":"%s"}`, id, id))
}
