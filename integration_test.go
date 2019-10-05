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
}`

var goChannelConfigWithTimeout = `
{
	"shutdown_timeout": 1,
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
}`

var sqsConfig = `
{
	"queues": [
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
	]
}`

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
type Basic struct{ ReturnCh chan string }

func (tj *Basic) Run(j *Job) error       { return nil }
func (tj *Basic) Done(j *Job, err error) { tj.ReturnCh <- j.Desc.Payload.(string) }

func TestBasicJob(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &Basic{ReturnCh: returnCh} })
	s, err := h.Queue("queue-1")
	if err != nil {
		t.Log(err)
		return
	}
	go h.Run()
	s.Send(getMessage(bodyTypeString, "foo"))
	assert.Equal(t, "foo", <-returnCh)
}

// ------------------------------------------------------------------

// Test done
type Done struct {
	ID       string
	ReturnCh chan string
}

func (t *Done) Run(j *Job) error       { t.ID = "foo"; return nil }
func (t *Done) Done(j *Job, err error) { t.ReturnCh <- t.ID }

func TestDoneJob(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &Done{ReturnCh: returnCh} })
	s, _ := h.Queue("queue-1")
	go h.Run()

	s.Send(getMessage(bodyTypeString, "foo"))
	assert.Equal(t, "foo", <-returnCh)
}

// ------------------------------------------------------------------

// Test err
type Err struct{ ReturnCh chan string }

func (t *Err) Run(j *Job) error       { return errors.New("error") }
func (t *Err) Done(j *Job, err error) { t.ReturnCh <- err.Error() }

func TestErrJob(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &Err{ReturnCh: returnCh} })
	s, _ := h.Queue("queue-1")
	go h.Run()

	s.Send(getMessage(bodyTypeString, "foo"))
	assert.Equal(t, "error", <-returnCh)
}

// ------------------------------------------------------------------

// Test pointer struct (run)
type PointerStructRun struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (tj *PointerStructRun) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload.(string)), &tj)
	time.Sleep(300 * time.Millisecond)
	tj.ReturnCh <- tj.ID
	return nil
}
func (tj *PointerStructRun) Done(j *Job, err error) {}

func TestPointerStructRunJob(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PointerStructRun{ReturnCh: returnCh} })
	s, _ := h.Queue("queue-1")
	go h.Run()

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
type PointerStructDone struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (tj *PointerStructDone) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload.(string)), &tj)
	time.Sleep(300 * time.Millisecond)
	return nil
}
func (tj *PointerStructDone) Done(j *Job, err error) { tj.ReturnCh <- tj.ID }

func TestPointerStructDoneJob(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PointerStructDone{ReturnCh: returnCh} })
	s, _ := h.Queue("queue-1")
	go h.Run()

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
type PointerStructCustom struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (tj *PointerStructCustom) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload.(string)), &tj)
	time.Sleep(300 * time.Millisecond)
	tj.Custom()
	return nil
}
func (tj *PointerStructCustom) Done(j *Job, err error) {}
func (tj *PointerStructCustom) Custom()                { tj.ReturnCh <- tj.ID }

func TestPointerStructCustomJob(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PointerStructCustom{ReturnCh: returnCh} })
	s, _ := h.Queue("queue-1")
	go h.Run()

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
type PointerStructDoneCustom struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (tj *PointerStructDoneCustom) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload.(string)), &tj)
	time.Sleep(300 * time.Millisecond)
	return nil
}
func (tj *PointerStructDoneCustom) Done(j *Job, err error) { tj.ID = tj.ID + "/done"; tj.Custom() }
func (tj *PointerStructDoneCustom) Custom()                { tj.ReturnCh <- tj.ID }

func TestPointerStructDoneCustomJob(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PointerStructDoneCustom{ReturnCh: returnCh} })
	s, _ := h.Queue("queue-1")
	go h.Run()

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
type PanicRun struct{}

func (tj *PanicRun) Run(j *Job) error       { panic("panic in Run"); return nil }
func (tj *PanicRun) Done(j *Job, err error) {}

func TestPanicRunJob(t *testing.T) {
	t.Parallel()
	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PanicRun{} })
	s, _ := h.Queue("queue-1")
	go func() {
		s.Send(getMessage(bodyTypeString, "foo"))
		time.Sleep(100 * time.Millisecond) // Give the worker some time to receive the message.
		h.Shutdown()
	}()
	h.Run()
	assert.Equal(t, int64(1), h.JobDoneCounter())
}

// ------------------------------------------------------------------

// Test panic in Done
type PanicDone struct{}

func (tj *PanicDone) Run(j *Job) error       { return nil }
func (tj *PanicDone) Done(j *Job, err error) { panic("panic in Done") }

func TestPanicDoneJob(t *testing.T) {
	t.Parallel()
	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PanicDone{} })
	s, _ := h.Queue("queue-1")
	go func() {
		s.Send(getMessage(bodyTypeString, "foo"))
		time.Sleep(100 * time.Millisecond) // Give the worker some time to receive the message.
		h.Shutdown()
	}()
	h.Run()
	assert.Equal(t, int64(1), h.JobDoneCounter())
}

// ------------------------------------------------------------------

// Test panic in Custom func
type PanicCustom struct{}

func (tj *PanicCustom) Run(j *Job) error       { return nil }
func (tj *PanicCustom) Done(j *Job, err error) { tj.Custom() }
func (tj *PanicCustom) Custom()                { panic("panic in Custom") }

func TestPanicCustomJob(t *testing.T) {
	t.Parallel()
	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PanicCustom{} })
	s, _ := h.Queue("queue-1")
	go func() {
		s.Send(getMessage(bodyTypeString, "foo"))
		time.Sleep(100 * time.Millisecond) // Give the worker some time to receive the message.
		h.Shutdown()
	}()
	h.Run()
	assert.Equal(t, int64(1), h.JobDoneCounter())
}

// ------------------------------------------------------------------

// Test go_channel
type GoChannel struct{}

func (tj *GoChannel) Run(j *Job) error       { return nil }
func (tj *GoChannel) Done(j *Job, err error) {}

func TestGoChannel100Jobs(t *testing.T) {
	t.Parallel()
	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &GoChannel{} })
	s, _ := h.Queue("queue-1")
	total := int64(100)

	go func() {
		for i := int64(0); i < total; i++ {
			s.Send(getMessage(bodyTypeString, strconv.FormatInt(i, 10)))
		}
		time.Sleep(100 * time.Millisecond) // Give the worker some time to receive the message.
		h.Shutdown()
	}()
	h.Run()
	assert.Equal(t, total, h.JobDoneCounter())
	t.Logf("counter/total: %d/%d\n", h.JobDoneCounter(), total)
}

// ------------------------------------------------------------------

// Test SQS
type SQS struct{}

func (tj *SQS) Run(j *Job) error       { return nil }
func (tj *SQS) Done(j *Job, err error) {}

func TestSqs100Jobs(t *testing.T) {
	h := New()
	h.InitWithJsonConfig(sqsConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &SQS{} })
	s, _ := h.Queue("queue-1")

	total := int64(100)
	go func() {
		for i := int64(0); i < total; i++ {
			s.Send([][]byte{getMessage(bodyTypeString, strconv.FormatInt(i, 10))})
		}
		time.Sleep(100 * time.Millisecond) // Give the worker some time to receive the message.
		h.Shutdown()
	}()
	h.Run()
	assert.Equal(t, total, h.JobDoneCounter())
	t.Logf("counter/total: %d/%d\n", h.JobDoneCounter(), total)
}

// ------------------------------------------------------------------

// Test graceful shutdown with 50k jobs
type GracefulShutdown struct{}

func (tj *GracefulShutdown) Run(j *Job) error       { return nil }
func (tj *GracefulShutdown) Done(j *Job, err error) {}

func TestGracefulShutdown(t *testing.T) {
	t.Parallel()
	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &GracefulShutdown{} })
	s, _ := h.Queue("queue-1")

	total := int64(50000)
	go func(total int64) {
		for i := int64(0); i < total; i++ {
			s.Send(getMessage(bodyTypeString, strconv.FormatInt(i, 10)))
		}
		time.Sleep(100 * time.Millisecond) // Give the worker some time to receive the message.
		h.Shutdown()
	}(total)
	h.Run()
	assert.Equal(t, total, h.JobDoneCounter())
	t.Logf("counter/total: %d/%d\n", h.JobDoneCounter(), total)
}

// ------------------------------------------------------------------

type ShutdownWithoutTimeout struct{}

func (tj *ShutdownWithoutTimeout) Run(j *Job) error       { time.Sleep(2 * time.Second); return nil }
func (tj *ShutdownWithoutTimeout) Done(j *Job, err error) {}

func TestShutdownWithoutTimeout(t *testing.T) {
	t.Parallel()
	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &ShutdownWithoutTimeout{} })
	s, _ := h.Queue("queue-1")

	total := int64(3)
	go func(total int64) {
		for i := int64(0); i < total; i++ {
			s.Send(getMessage(bodyTypeString, strconv.FormatInt(i, 10)))
		}
		time.Sleep(100 * time.Millisecond) // Give the worker some time to receive the message.
		h.Shutdown()
	}(total)
	start := time.Now()
	h.Run()
	end := time.Now()

	// Jobs don't have enough time to be finished
	assert.WithinDuration(t, start, end, 2200*time.Millisecond) // 2s processing time + 100ms sleep + 100ms launching time
	assert.Equal(t, int64(3), h.JobDoneCounter())
}

// ------------------------------------------------------------------

type ShutdownWithTimeout struct{}

func (tj *ShutdownWithTimeout) Run(j *Job) error       { time.Sleep(10 * time.Second); return nil }
func (tj *ShutdownWithTimeout) Done(j *Job, err error) {}

func TestShutdownWithTimeout(t *testing.T) {
	t.Parallel()
	h := New()
	h.InitWithJsonConfig(goChannelConfigWithTimeout)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &ShutdownWithTimeout{} })
	s, _ := h.Queue("queue-1")

	total := int64(3)
	go func(total int64) {
		for i := int64(0); i < total; i++ {
			s.Send(getMessage(bodyTypeString, strconv.FormatInt(i, 10)))
		}
		time.Sleep(100 * time.Millisecond) // Give the worker some time to receive the message.
		h.Shutdown()
	}(total)
	start := time.Now()
	h.Run()
	end := time.Now()

	// Jobs don't have enough time to be finished
	assert.WithinDuration(t, start, end, 1200*time.Millisecond) // 1s timeout + 100ms sleep + 100ms launching time
	assert.Equal(t, int64(0), h.JobDoneCounter())
}

// ------------------------------------------------------------------
