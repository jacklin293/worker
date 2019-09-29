// +build integration

package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

var sqsConfig = `[
	{
		"name":"queue-1",
		"queue_type":"sqs",
		"queue_concurrency": 3,
		"worker_concurrency":100,
		"enabled":true,
		"sqs": {
			"queue_url": "http://localhost:4100/100010001000/integration-test",
			"use_local_sqs": true,
			"region": "us-east-1",
			"max_number_of_messages": 2,
			"wait_time_seconds": 2
		}
	}
]`

func getMessage(id string) []byte {
	return []byte(fmt.Sprintf(`{"job_id":"test-job-id-%s","job_type":"test-job-type-1","payload":"{\"id\":\"%s\",\"timestamp\":%d}"}`, id, id, time.Now().UnixNano()))
}

// ------------------------------------------------------------------

// Test basic function
type TestBasic struct{}

func (tj *TestBasic) Run(j *Job) error       { return nil }
func (tj *TestBasic) Done(j *Job, err error) {}

func TestBasicJob(t *testing.T) {
	doneCh := make(chan *Job)

	// New handler
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.SetNotifyChan(doneCh)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process {
		return &TestBasic{}
	})
	m.Run()
	s, _ := m.GetQueueByName("queue-1")

	// Prepare the message and expected job struct
	msg := getMessage("foo")
	expected := Job{}
	json.Unmarshal(msg, &expected.Desc)

	// Send the message
	s.Send(msg)
	time.Sleep(1 * time.Millisecond)

	// Returned job (converted from message)
	result := <-doneCh
	assert.Equal(t, expected.Desc.Payload, result.Desc.Payload)
}

// ------------------------------------------------------------------

// Test done
type TestDone struct {
	ID       string
	ReturnCh chan string
}

func (t *TestDone) Run(j *Job) error {
	t.ID = "foo"
	return nil
}
func (t *TestDone) Done(j *Job, err error) { t.ReturnCh <- t.ID }

func TestDoneJob(t *testing.T) {
	doneCh := make(chan *Job)
	returnCh := make(chan string)

	// New handler
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.SetNotifyChan(doneCh)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process {
		return &TestDone{ReturnCh: returnCh}
	})
	m.Run()
	s, _ := m.GetQueueByName("queue-1")

	// Send the message
	s.Send(getMessage("foo"))
	time.Sleep(1 * time.Millisecond)
	assert.Equal(t, "foo", <-returnCh)
	<-doneCh
}

// ------------------------------------------------------------------

// Test err
type TestErr struct {
	ReturnCh chan string
}

func (t *TestErr) Run(j *Job) error       { return errors.New("bar") }
func (t *TestErr) Done(j *Job, err error) { t.ReturnCh <- err.Error() }

func TestErrJob(t *testing.T) {
	doneCh := make(chan *Job)
	returnCh := make(chan string)

	// New handler
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.SetNotifyChan(doneCh)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process {
		return &TestErr{ReturnCh: returnCh}
	})
	m.Run()
	s, _ := m.GetQueueByName("queue-1")

	// Send the message
	s.Send(getMessage("foo"))
	time.Sleep(1 * time.Millisecond)
	assert.Equal(t, "bar", <-returnCh)
	<-doneCh
}

// ------------------------------------------------------------------

// Test pointer misuse (run)
type TestStructPointerMisuseRun struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (tj *TestStructPointerMisuseRun) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload.(string)), &tj)
	time.Sleep(300 * time.Millisecond)
	tj.ReturnCh <- tj.ID
	return nil
}
func (tj *TestStructPointerMisuseRun) Done(j *Job, err error) {}

func TestStructPointerMisuseRunJob(t *testing.T) {
	doneCh := make(chan *Job)
	returnCh := make(chan string)

	// New handler
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.SetNotifyChan(doneCh)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process {
		return &TestStructPointerMisuseRun{ReturnCh: returnCh}
	})
	m.Run()
	s, _ := m.GetQueueByName("queue-1")

	// Expected ID
	expectedID1 := "foo"
	expectedID2 := "bar"

	// Send the messages separately with 150ms delay
	s.Send(getMessage(expectedID1))
	time.Sleep(150 * time.Millisecond)
	s.Send(getMessage(expectedID2))
	assert.Equal(t, expectedID1, <-returnCh)
	assert.Equal(t, expectedID2, <-returnCh)
	<-doneCh
	<-doneCh
}

// ------------------------------------------------------------------

// Test pointer misuse (done)
type TestStructPointerMisuseDone struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (tj *TestStructPointerMisuseDone) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload.(string)), &tj)
	time.Sleep(300 * time.Millisecond)
	return nil
}
func (tj *TestStructPointerMisuseDone) Done(j *Job, err error) { tj.ReturnCh <- tj.ID }

func TestStructPointerMisuseDoneJob(t *testing.T) {
	doneCh := make(chan *Job)
	returnCh := make(chan string)

	// New handler
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.SetNotifyChan(doneCh)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process {
		return &TestStructPointerMisuseDone{ReturnCh: returnCh}
	})
	m.Run()
	s, _ := m.GetQueueByName("queue-1")

	// Expected ID
	expectedID1 := "foo"
	expectedID2 := "bar"

	// Send the messages separately with 150ms delay
	s.Send(getMessage(expectedID1))
	time.Sleep(150 * time.Millisecond)
	s.Send(getMessage(expectedID2))
	assert.Equal(t, expectedID1, <-returnCh)
	assert.Equal(t, expectedID2, <-returnCh)
	<-doneCh
	<-doneCh
}

// ------------------------------------------------------------------

// Test pointer misuse (custom)
type TestStructPointerMisuseCustom struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (tj *TestStructPointerMisuseCustom) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload.(string)), &tj)
	time.Sleep(300 * time.Millisecond)
	tj.Custom()
	return nil
}
func (tj *TestStructPointerMisuseCustom) Done(j *Job, err error) {}
func (tj *TestStructPointerMisuseCustom) Custom()                { tj.ReturnCh <- tj.ID }

func TestStructPointerMisuseCustomJob(t *testing.T) {
	doneCh := make(chan *Job)
	returnCh := make(chan string)

	// New handler
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.SetNotifyChan(doneCh)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process {
		return &TestStructPointerMisuseCustom{ReturnCh: returnCh}
	})
	m.Run()
	s, _ := m.GetQueueByName("queue-1")

	// Expected ID
	expectedID1 := "foo"
	expectedID2 := "bar"

	// Send the messages separately with 150ms delay
	s.Send(getMessage(expectedID1))
	time.Sleep(150 * time.Millisecond)
	s.Send(getMessage(expectedID2))
	assert.Equal(t, expectedID1, <-returnCh)
	assert.Equal(t, expectedID2, <-returnCh)
	<-doneCh
	<-doneCh
}

// ------------------------------------------------------------------

// Test pointer misuse (custom)
type TestStructPointerMisuseDoneCustom struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (tj *TestStructPointerMisuseDoneCustom) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload.(string)), &tj)
	time.Sleep(300 * time.Millisecond)
	return nil
}
func (tj *TestStructPointerMisuseDoneCustom) Done(j *Job, err error) {
	tj.ID = tj.ID + "/done"
	tj.Custom()
}
func (tj *TestStructPointerMisuseDoneCustom) Custom() { tj.ReturnCh <- tj.ID }

func TestStructPointerMisuseDoneCustomJob(t *testing.T) {
	doneCh := make(chan *Job)
	returnCh := make(chan string)

	// New handler
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.SetNotifyChan(doneCh)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process {
		return &TestStructPointerMisuseDoneCustom{ReturnCh: returnCh}
	})
	m.Run()
	s, _ := m.GetQueueByName("queue-1")

	// Expected ID
	expectedID1 := "foo"
	expectedID2 := "bar"

	// Send the messages separately with 150ms delay
	s.Send(getMessage(expectedID1))
	time.Sleep(150 * time.Millisecond)
	s.Send(getMessage(expectedID2))
	assert.Equal(t, expectedID1+"/done", <-returnCh)
	assert.Equal(t, expectedID2+"/done", <-returnCh)
	<-doneCh
	<-doneCh
}

// ------------------------------------------------------------------

// Test panic in Run
type TestPanicRun struct{}

func (tj *TestPanicRun) Run(j *Job) error {
	panic("panic in Run")
	return nil
}
func (tj *TestPanicRun) Done(j *Job, err error) {}

func TestPanicRunJob(t *testing.T) {
	doneCh := make(chan *Job)

	// New handler
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.SetNotifyChan(doneCh)
	m.Run()
	s, _ := m.GetQueueByName("queue-1")
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process {
		return &TestPanicRun{}
	})

	// Prepare the message and expected job struct
	msg := getMessage("foo")
	expected := Job{}
	json.Unmarshal(msg, &expected.Desc)

	// Send the message
	s.Send(msg)
	time.Sleep(1 * time.Millisecond)

	// Returned job (converted from message)
	result := <-doneCh
	assert.Equal(t, expected.Desc.Payload, result.Desc.Payload)
}

// ------------------------------------------------------------------

// Test panic in Done
type TestPanicDone struct{}

func (tj *TestPanicDone) Run(j *Job) error       { return nil }
func (tj *TestPanicDone) Done(j *Job, err error) { panic("panic in Done") }

func TestPanicDoneJob(t *testing.T) {
	doneCh := make(chan *Job)

	// New handler
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.SetNotifyChan(doneCh)
	m.Run()
	s, _ := m.GetQueueByName("queue-1")
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process {
		return &TestPanicDone{}
	})

	// Prepare the message and expected job struct
	msg := getMessage("foo")
	expected := Job{}
	json.Unmarshal(msg, &expected.Desc)

	// Send the message
	s.Send(msg)
	time.Sleep(1 * time.Millisecond)

	// Returned job (converted from message)
	result := <-doneCh
	assert.Equal(t, expected.Desc.Payload, result.Desc.Payload)
}

// ------------------------------------------------------------------

// Test panic in Done
type TestPanicCustom struct{}

func (tj *TestPanicCustom) Run(j *Job) error       { return nil }
func (tj *TestPanicCustom) Done(j *Job, err error) { tj.Custom() }
func (tj *TestPanicCustom) Custom()                { panic("panic in Custom") }

func TestPanicCustomJob(t *testing.T) {
	doneCh := make(chan *Job)

	// New handler
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.SetNotifyChan(doneCh)
	m.Run()
	s, _ := m.GetQueueByName("queue-1")
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process {
		return &TestPanicCustom{}
	})

	// Prepare the message and expected job struct
	msg := getMessage("foo")
	expected := Job{}
	json.Unmarshal(msg, &expected.Desc)

	// Send the message
	s.Send(msg)
	time.Sleep(1 * time.Millisecond)

	// Returned job (converted from message)
	result := <-doneCh
	assert.Equal(t, expected.Desc.Payload, result.Desc.Payload)
}

// ------------------------------------------------------------------

// Test GoChannel 50k jobs
func TestGoChannel50kJobs(t *testing.T) {
	doneCh := make(chan *Job)

	// New handler
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.SetNotifyChan(doneCh)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process {
		return &TestBasic{}
	})
	m.Run()
	s, err := m.GetQueueByName("queue-1")
	if err != nil {
		t.Log(err)
		return
	}

	// Send
	var wg sync.WaitGroup
	counter := 0
	total := 50000
	go func(wg *sync.WaitGroup, total int) {
		for i := 0; i < total; i++ {
			wg.Add(1)
			go func(i int) {
				s.Send(getMessage(strconv.Itoa(i)))
			}(i)
		}
	}(&wg, total)

	// Let wg.Add works before it ends
	time.Sleep(1 * time.Millisecond)

	// Receive
	go func(wg *sync.WaitGroup, counter *int) {
		for i := 0; i < total; i++ {
			select {
			case <-doneCh:
				*counter++
				wg.Done()
			}
		}
	}(&wg, &counter)
	wg.Wait()

	assert.Equal(t, total, counter)
	t.Logf("counter/total: %d/%d\n", counter, total)
}

// ------------------------------------------------------------------

// Test SQS 500 jobs
// FIXME up to 50,000
func TestSqs500Jobs(t *testing.T) {
	doneCh := make(chan *Job)

	// New handler
	m := New()
	m.InitWithJsonConfig(sqsConfig)
	m.SetNotifyChan(doneCh)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process {
		return &TestBasic{}
	})
	m.Run()
	s, err := m.GetQueueByName("queue-1")
	assert.NotNil(t, s)
	if !assert.NoError(t, err) {
		return
	}

	// Send
	var wg sync.WaitGroup
	counter := 0
	total := 500
	go func(wg *sync.WaitGroup, total int) {
		for i := 0; i < total; i++ {
			wg.Add(1)
			go func(i int) {
				_, err := s.Send([][]byte{getMessage(strconv.Itoa(i))})
				if !assert.NoError(t, err) {
					t.Log(err)
					return
				}
			}(i)
		}
	}(&wg, total)

	// Let wg.Add works before it ends
	time.Sleep(1 * time.Millisecond)

	// Receive
	go func(wg *sync.WaitGroup, counter *int) {
		for i := 0; i < total; i++ {
			select {
			case <-doneCh:
				*counter++
				wg.Done()
			}
		}
	}(&wg, &counter)
	wg.Wait()

	assert.Equal(t, total, counter)
	t.Logf("counter/total: %d/%d\n", counter, total)
}

// ------------------------------------------------------------------
