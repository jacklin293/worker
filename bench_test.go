// +build bench

package worker

import (
	"fmt"
	"testing"
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
	ID       string `json:"id"`
	Now      int64
	ReturnCh chan string
}

// Test Race condition
func (tj *TestBasic) Run(j *Job) error       { return nil }
func (tj *TestBasic) Done(j *Job, err error) { tj.ReturnCh <- j.Desc.Payload.(string) }

func BenchmarkLoops(b *testing.B) {
	returnCh := make(chan string)

	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &TestBasic{ReturnCh: returnCh} })
	source, _ := m.Queue("queue-1")
	go m.Run()
	for i := 0; i <= b.N; i++ {
		go func() {
			for i := 0; i < 1000; i++ {
				source.Send([]byte("{\"job_id\":\"test-job-id-dd\",\"job_type\":\"test-job-type-1\",\"payload\":\"dd\"}"))
			}
		}()
		for i := 0; i < 1000; i++ {
			<-returnCh
		}
	}
	m.Shutdown()
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
