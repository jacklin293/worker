// +build integration

package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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

type JobBodyType string

const (
	bodyTypeString JobBodyType = "string"
	bodyTypeMap    JobBodyType = "map"
)

func getMessage(t JobBodyType, id string) []byte {
	switch t {
	case bodyTypeString:
		return []byte(fmt.Sprintf(`{"job_id":"test-job-id-%s","job_type":"test-job-type-1","payload":"%s"}`, id, id))
	case bodyTypeMap:
		return []byte(fmt.Sprintf(`{"job_id":"test-job-id-%s","job_type":"test-job-type-1","payload":"{\"id\":\"%s\",\"timestamp\":%d}"}`, id, id, time.Now().UnixNano()))
	}
	return []byte{}
}

// ------------------------------------------------------------------

// Test basic function
type TestBasic struct{ ReturnCh chan string }

func (tj *TestBasic) Run(j *Job) error       { return nil }
func (tj *TestBasic) Done(j *Job, err error) { tj.ReturnCh <- j.Desc.Payload.(string) }

func TestBasicJob(t *testing.T) {
	returnCh := make(chan string)

	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &TestBasic{ReturnCh: returnCh} })
	s, err := m.Queue("queue-1")
	if err != nil {
		t.Log(err)
		return
	}
	go m.Run()

	s.Send(getMessage(bodyTypeString, "foo"))
	assert.Equal(t, "foo", <-returnCh)
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
	returnCh := make(chan string)

	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &TestDone{ReturnCh: returnCh} })
	s, _ := m.Queue("queue-1")
	go m.Run()

	s.Send(getMessage(bodyTypeString, "foo"))
	assert.Equal(t, "foo", <-returnCh)
}

// ------------------------------------------------------------------

// Test err
type TestErr struct {
	ReturnCh chan string
}

func (t *TestErr) Run(j *Job) error       { return errors.New("error") }
func (t *TestErr) Done(j *Job, err error) { t.ReturnCh <- err.Error() }

func TestErrJob(t *testing.T) {
	returnCh := make(chan string)

	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &TestErr{ReturnCh: returnCh} })
	s, _ := m.Queue("queue-1")
	go m.Run()

	s.Send(getMessage(bodyTypeString, "foo"))
	assert.Equal(t, "error", <-returnCh)
}

// ------------------------------------------------------------------

// Test pointer struct (run)
type TestPointerStructRun struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (tj *TestPointerStructRun) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload.(string)), &tj)
	time.Sleep(300 * time.Millisecond)
	tj.ReturnCh <- tj.ID
	return nil
}
func (tj *TestPointerStructRun) Done(j *Job, err error) {}

func TestPointerStructRunJob(t *testing.T) {
	returnCh := make(chan string)

	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &TestPointerStructRun{ReturnCh: returnCh} })
	s, _ := m.Queue("queue-1")
	go m.Run()

	expectedID1 := "foo"
	expectedID2 := "bar"
	s.Send(getMessage(bodyTypeMap, expectedID1))
	time.Sleep(150 * time.Millisecond)
	s.Send(getMessage(bodyTypeMap, expectedID2))
	assert.Equal(t, expectedID1, <-returnCh)
	assert.Equal(t, expectedID2, <-returnCh)
}

// ------------------------------------------------------------------

// Test pointer struct (done)
type TestPointerStructDone struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (tj *TestPointerStructDone) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload.(string)), &tj)
	time.Sleep(300 * time.Millisecond)
	return nil
}
func (tj *TestPointerStructDone) Done(j *Job, err error) { tj.ReturnCh <- tj.ID }

func TestPointerStructDoneJob(t *testing.T) {
	returnCh := make(chan string)

	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &TestPointerStructDone{ReturnCh: returnCh} })
	s, _ := m.Queue("queue-1")
	go m.Run()

	expectedID1 := "foo"
	expectedID2 := "bar"
	s.Send(getMessage(bodyTypeMap, expectedID1))
	time.Sleep(150 * time.Millisecond)
	s.Send(getMessage(bodyTypeMap, expectedID2))
	assert.Equal(t, expectedID1, <-returnCh)
	assert.Equal(t, expectedID2, <-returnCh)
}

// ------------------------------------------------------------------

// Test pointer struct (custom)
type TestPointerStructCustom struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (tj *TestPointerStructCustom) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload.(string)), &tj)
	time.Sleep(300 * time.Millisecond)
	tj.Custom()
	return nil
}
func (tj *TestPointerStructCustom) Done(j *Job, err error) {}
func (tj *TestPointerStructCustom) Custom()                { tj.ReturnCh <- tj.ID }

func TestPointerStructCustomJob(t *testing.T) {
	returnCh := make(chan string)

	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &TestPointerStructCustom{ReturnCh: returnCh} })
	s, _ := m.Queue("queue-1")
	go m.Run()

	expectedID1 := "foo"
	expectedID2 := "bar"
	s.Send(getMessage(bodyTypeMap, expectedID1))
	time.Sleep(150 * time.Millisecond)
	s.Send(getMessage(bodyTypeMap, expectedID2))
	assert.Equal(t, expectedID1, <-returnCh)
	assert.Equal(t, expectedID2, <-returnCh)
}

// ------------------------------------------------------------------

// Test pointer struct (done->custom)
type TestPointerStructDoneCustom struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (tj *TestPointerStructDoneCustom) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload.(string)), &tj)
	time.Sleep(300 * time.Millisecond)
	return nil
}
func (tj *TestPointerStructDoneCustom) Done(j *Job, err error) {
	tj.ID = tj.ID + "/done"
	tj.Custom()
}
func (tj *TestPointerStructDoneCustom) Custom() { tj.ReturnCh <- tj.ID }

func TestPointerStructDoneCustomJob(t *testing.T) {
	returnCh := make(chan string)

	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &TestPointerStructDoneCustom{ReturnCh: returnCh} })
	s, _ := m.Queue("queue-1")
	go m.Run()

	expectedID1 := "foo"
	expectedID2 := "bar"
	s.Send(getMessage(bodyTypeMap, expectedID1))
	time.Sleep(150 * time.Millisecond)
	s.Send(getMessage(bodyTypeMap, expectedID2))
	assert.Equal(t, expectedID1+"/done", <-returnCh)
	assert.Equal(t, expectedID2+"/done", <-returnCh)
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
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &TestPanicRun{} })
	s, _ := m.Queue("queue-1")
	go m.Run()

	s.Send(getMessage(bodyTypeString, "foo"))
	time.Sleep(10 * time.Millisecond) // Let worker process the job
	assert.Equal(t, int64(1), m.JobCounter())
}

// ------------------------------------------------------------------

// Test panic in Done
type TestPanicDone struct{}

func (tj *TestPanicDone) Run(j *Job) error       { return nil }
func (tj *TestPanicDone) Done(j *Job, err error) { panic("panic in Done") }

func TestPanicDoneJob(t *testing.T) {
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &TestPanicDone{} })
	s, _ := m.Queue("queue-1")
	go m.Run()

	s.Send(getMessage(bodyTypeString, "foo"))
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, int64(1), m.JobCounter())
}

// ------------------------------------------------------------------

// Test panic in Custom func
type TestPanicCustom struct{}

func (tj *TestPanicCustom) Run(j *Job) error       { return nil }
func (tj *TestPanicCustom) Done(j *Job, err error) { tj.Custom() }
func (tj *TestPanicCustom) Custom()                { panic("panic in Custom") }

func TestPanicCustomJob(t *testing.T) {
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &TestPanicCustom{} })
	s, _ := m.Queue("queue-1")
	go m.Run()

	s.Send(getMessage(bodyTypeString, "foo"))
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, int64(1), m.JobCounter())
}

// ------------------------------------------------------------------

// Test go_channel
type TestGoChannel struct{}

func (tj *TestGoChannel) Run(j *Job) error       { return nil }
func (tj *TestGoChannel) Done(j *Job, err error) {}

func TestGoChannel100Jobs(t *testing.T) {
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &TestGoChannel{} })
	s, _ := m.Queue("queue-1")
	go m.Run()

	total := int64(100)
	go func(total int64) {
		for i := int64(0); i < total; i++ {
			go func(i int64) {
				s.Send(getMessage(bodyTypeString, strconv.FormatInt(i, 10)))
			}(i)
		}
	}(total)

	var counter int64
	for counter < total {
		time.Sleep(10 * time.Millisecond)
		counter = m.JobCounter()
	}
	assert.Equal(t, total, counter)
	t.Logf("counter/total: %d/%d\n", counter, total)
}

// ------------------------------------------------------------------

// Test SQS
type TestSQS struct{}

func (tj *TestSQS) Run(j *Job) error       { return nil }
func (tj *TestSQS) Done(j *Job, err error) {}

func TestSqs100Jobs(t *testing.T) {
	m := New()
	m.InitWithJsonConfig(sqsConfig)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &TestSQS{} })
	s, _ := m.Queue("queue-1")
	go m.Run()

	total := int64(100)
	go func(total int64) {
		for i := int64(0); i < total; i++ {
			go func(i int64) {
				s.Send([][]byte{getMessage(bodyTypeString, strconv.FormatInt(i, 10))})
			}(i)
		}
	}(total)

	var counter int64
	for counter < total {
		time.Sleep(10 * time.Millisecond)
		counter = m.JobCounter()
	}
	assert.Equal(t, total, counter)
	t.Logf("counter/total: %d/%d\n", counter, total)
}

// ------------------------------------------------------------------

// Test graceful shutdown with 50k jobs
type TestGracefulShutdown struct{}

func (tj *TestGracefulShutdown) Run(j *Job) error       { return nil }
func (tj *TestGracefulShutdown) Done(j *Job, err error) {}

func TestTestGracefulShutdown(t *testing.T) {
	m := New()
	m.InitWithJsonConfig(goChannelConfig)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &TestGracefulShutdown{} })
	s, _ := m.Queue("queue-1")
	go m.Run()

	total := int64(50000)
	go func(total int64) {
		for i := int64(0); i < total; i++ {
			go func(i int64) {
				s.Send(getMessage(bodyTypeString, strconv.FormatInt(i, 10)))
			}(i)
		}
	}(total)

	var counter int64
	for counter < total {
		time.Sleep(10 * time.Millisecond)
		counter = m.JobCounter()
	}
	assert.Equal(t, total, counter)
	t.Logf("counter/total: %d/%d\n", counter, total)
}

// ------------------------------------------------------------------
