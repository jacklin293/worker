// +build integration

package worker

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

type TestJob struct{}

type TestRaceRun struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

type TestRaceDone struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func getMessage(id string) []byte {
	return []byte(fmt.Sprintf(`{"job_id":"test-job-id-%s","job_type":"test-job-type-1","payload":"{\"id\":\"%s\",\"now\":%d}"}`, id, id, time.Now().Unix()))
}

func (tj *TestJob) Run(j *Job) error       { return nil }
func (tj *TestJob) Done(j *Job, err error) {}

func TestOneJob(t *testing.T) {
	doneCh := make(chan *Job)

	// New handler
	m := New()
	m.SetConfigWithJSON(singleTopicConfig)
	m.SetNotifyChan(doneCh)
	m.Run()
	source, _ := m.GetSourceByName("queue-1")

	// Initialise job
	m.RegisterJobType("queue-1", "test-job-type-1", func() Contract {
		return &TestJob{}
	})

	// Expected msg
	msg := getMessage("foo")
	expected := Job{}
	json.Unmarshal(msg, &expected.Desc)

	// Send the message
	source.Send(msg)
	time.Sleep(1 * time.Millisecond)

	// Returned job (converted from message)
	result := <-doneCh

	assert.Equal(t, expected.Desc.Payload, result.Desc.Payload)
}

// Test Race condition
func (tj *TestRaceRun) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload), &tj)
	time.Sleep(300 * time.Millisecond)
	tj.ReturnCh <- tj.ID
	return nil
}
func (tj *TestRaceRun) Done(j *Job, err error) {}

func TestRaceConditionRun(t *testing.T) {
	doneCh := make(chan *Job)

	// New handler
	m := New()
	m.SetConfigWithJSON(singleTopicConfig)
	m.SetNotifyChan(doneCh)
	m.Run()
	source, _ := m.GetSourceByName("queue-1")

	// Initialise job
	returnCh := make(chan string)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Contract {
		return &TestRaceRun{ReturnCh: returnCh}
	})

	// Expected ID
	expectedID1 := "foo"
	expectedID2 := "bar"

	// Send the messages separately with 150ms delay
	source.Send(getMessage(expectedID1))
	time.Sleep(150 * time.Millisecond)
	source.Send(getMessage(expectedID2))

	assert.Equal(t, expectedID1, <-returnCh)
	assert.Equal(t, expectedID2, <-returnCh)
	<-doneCh
	<-doneCh
}

// Test Race condition
func (tj *TestRaceDone) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload), &tj)
	time.Sleep(300 * time.Millisecond)
	return nil
}
func (tj *TestRaceDone) Done(j *Job, err error) {
	tj.ReturnCh <- tj.ID
}

func TestRaceConditionDone(t *testing.T) {
	doneCh := make(chan *Job)

	// New handler
	m := New()
	m.SetConfigWithJSON(singleTopicConfig)
	m.SetNotifyChan(doneCh)
	m.Run()
	source, _ := m.GetSourceByName("queue-1")

	// Initialise job
	returnCh := make(chan string)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Contract {
		return &TestRaceDone{ReturnCh: returnCh}
	})

	// Expected ID
	expectedID1 := "foo"
	expectedID2 := "bar"

	// Send the messages separately with 150ms delay
	source.Send(getMessage(expectedID1))
	time.Sleep(150 * time.Millisecond)
	source.Send(getMessage(expectedID2))

	assert.Equal(t, expectedID1, <-returnCh)
	assert.Equal(t, expectedID2, <-returnCh)
	<-doneCh
	<-doneCh
}

func Test50kJobs(t *testing.T) {
	doneCh := make(chan *Job)

	// New handler
	m := New()
	m.SetConfigWithJSON(singleTopicConfig)
	m.SetNotifyChan(doneCh)
	m.Run()
	source, _ := m.GetSourceByName("queue-1")

	// Initialise job
	m.RegisterJobType("queue-1", "test-job-type-1", func() Contract {
		return &TestJob{}
	})

	//
	var wg sync.WaitGroup
	counter := 0
	total := 50000
	go func(wg *sync.WaitGroup, total int) {
		for i := 0; i < total; i++ {
			wg.Add(1)
			go func(i int) {
				source.Send(getMessage(strconv.Itoa(i)))
			}(i)
		}
	}(&wg, total)

	// Let wg.Add works before it ends
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

	assert.Equal(t, total, counter)
}
