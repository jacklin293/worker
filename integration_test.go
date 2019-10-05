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
		return []byte(fmt.Sprintf(`{"id":"test-job-id-%s","type":"test-job-type-1","payload":"%s"}`, id, id))
	case bodyTypeMap:
		return []byte(fmt.Sprintf(`{"id":"test-job-id-%s","type":"test-job-type-1","payload":"{\"id\":\"%s\",\"timestamp\":%d}"}`, id, id, time.Now().UnixNano()))
	}
	return []byte{}
}

// ------------------------------------------------------------------

// Test basic function
type Run struct{ ReturnCh chan string }

func (jt *Run) Run(j *Job) error       { jt.ReturnCh <- "foo"; return nil }
func (jt *Run) Done(j *Job, err error) {}

func TestRun(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &Run{ReturnCh: returnCh} })
	s, _ := h.Queue("queue-1")
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

func TestDone(t *testing.T) {
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

func TestErr(t *testing.T) {
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

type JobStructRun struct{ ReturnCh chan interface{} }

func (jt *JobStructRun) Run(j *Job) error {
	jt.ReturnCh <- j.Id()
	jt.ReturnCh <- j.Type()
	jt.ReturnCh <- j.Payload()
	jt.ReturnCh <- j.ReceivedAt().IsZero()
	jt.ReturnCh <- j.DoneAt().IsZero()
	jt.ReturnCh <- j.Duration()
	return nil
}
func (jt *JobStructRun) Done(j *Job, err error) {}

func TestJobStructRun(t *testing.T) {
	t.Parallel()
	returnCh := make(chan interface{})

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &JobStructRun{ReturnCh: returnCh} })
	s, err := h.Queue("queue-1")
	if err != nil {
		t.Log(err)
		return
	}
	go h.Run()
	s.Send(getMessage(bodyTypeString, "foo"))
	id := <-returnCh
	assert.Equal(t, "test-job-id-foo", id.(string))
	typ := <-returnCh
	assert.Equal(t, "test-job-type-1", typ.(string))
	payload := <-returnCh
	assert.Equal(t, "foo", payload.(string))
	receivedAt := <-returnCh
	assert.Equal(t, false, receivedAt.(bool))
	doneAt := <-returnCh
	assert.Equal(t, true, doneAt.(bool))
	duration := <-returnCh
	assert.Equal(t, "0s", duration.(time.Duration).String())
}

// ------------------------------------------------------------------

type JobStructDone struct{ ReturnCh chan interface{} }

func (jt *JobStructDone) Run(j *Job) error { jt.ReturnCh <- j.Payload(); return nil }
func (jt *JobStructDone) Done(j *Job, err error) {
	jt.ReturnCh <- j.Id()
	jt.ReturnCh <- j.Type()
	jt.ReturnCh <- j.Payload()
	jt.ReturnCh <- j.ReceivedAt().IsZero()
	jt.ReturnCh <- j.DoneAt().IsZero()
	jt.ReturnCh <- j.Duration()
}

func TestJobStructDone(t *testing.T) {
	t.Parallel()
	returnCh := make(chan interface{})

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &JobStructDone{ReturnCh: returnCh} })
	s, err := h.Queue("queue-1")
	if err != nil {
		t.Log(err)
		return
	}
	go h.Run()
	payload := getMessage(bodyTypeMap, "foo")
	s.Send(payload)

	job := map[string]interface{}{}
	json.Unmarshal(payload, &job)

	// Test case of job payload using map
	payloadRun := <-returnCh
	assert.Equal(t, job["payload"], payloadRun)

	id := <-returnCh
	assert.Equal(t, job["id"], id)
	typ := <-returnCh
	assert.Equal(t, job["type"], typ)
	payloadDone := <-returnCh
	assert.Equal(t, job["payload"], payloadDone)
	receivedAt := <-returnCh
	assert.Equal(t, false, receivedAt.(bool))
	doneAt := <-returnCh
	assert.Equal(t, false, doneAt.(bool))
	duration := <-returnCh
	assert.NotEqual(t, "0s", duration.(time.Duration).String())
}

// ------------------------------------------------------------------

// Test pointer job type (run)
// Not only does it test job type, it also test job struct
type PointerJobTypeRun struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (jt *PointerJobTypeRun) Run(j *Job) error {
	json.Unmarshal([]byte(j.Payload().(string)), &jt)
	time.Sleep(300 * time.Millisecond)
	jt.ReturnCh <- jt.ID
	return nil
}
func (jt *PointerJobTypeRun) Done(j *Job, err error) {}

func TestPointerJobTypeRun(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PointerJobTypeRun{ReturnCh: returnCh} })
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

// Test pointer job type (done)
type PointerJobTypeDone struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (jt *PointerJobTypeDone) Run(j *Job) error {
	json.Unmarshal([]byte(j.Payload().(string)), &jt)
	time.Sleep(300 * time.Millisecond)
	return nil
}
func (jt *PointerJobTypeDone) Done(j *Job, err error) { jt.ReturnCh <- jt.ID }

func TestPointerJobTypeDone(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PointerJobTypeDone{ReturnCh: returnCh} })
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
type PointerJobTypeCustom struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (jt *PointerJobTypeCustom) Run(j *Job) error {
	json.Unmarshal([]byte(j.Payload().(string)), &jt)
	time.Sleep(300 * time.Millisecond)
	jt.Custom()
	return nil
}
func (jt *PointerJobTypeCustom) Done(j *Job, err error) {}
func (jt *PointerJobTypeCustom) Custom()                { jt.ReturnCh <- jt.ID }

func TestPointerStructCustom(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PointerJobTypeCustom{ReturnCh: returnCh} })
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
type PointerJobTypeDoneCustom struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (jt *PointerJobTypeDoneCustom) Run(j *Job) error {
	json.Unmarshal([]byte(j.Payload().(string)), &jt)
	time.Sleep(300 * time.Millisecond)
	return nil
}
func (jt *PointerJobTypeDoneCustom) Done(j *Job, err error) { jt.ID = jt.ID + "/done"; jt.Custom() }
func (jt *PointerJobTypeDoneCustom) Custom()                { jt.ReturnCh <- jt.ID }

func TestPointerStructDoneCustom(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PointerJobTypeDoneCustom{ReturnCh: returnCh} })
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

func (jt *PanicRun) Run(j *Job) error       { panic("panic in Run"); return nil }
func (jt *PanicRun) Done(j *Job, err error) {}

func TestPanicRun(t *testing.T) {
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

func (jt *PanicDone) Run(j *Job) error       { return nil }
func (jt *PanicDone) Done(j *Job, err error) { panic("panic in Done") }

func TestPanicDone(t *testing.T) {
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

func (jt *PanicCustom) Run(j *Job) error       { return nil }
func (jt *PanicCustom) Done(j *Job, err error) { jt.Custom() }
func (jt *PanicCustom) Custom()                { panic("panic in Custom") }

func TestPanicCustom(t *testing.T) {
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

func (jt *GoChannel) Run(j *Job) error       { return nil }
func (jt *GoChannel) Done(j *Job, err error) {}

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

func (jt *SQS) Run(j *Job) error       { return nil }
func (jt *SQS) Done(j *Job, err error) {}

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

func (jt *GracefulShutdown) Run(j *Job) error       { return nil }
func (jt *GracefulShutdown) Done(j *Job, err error) {}

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

func (jt *ShutdownWithoutTimeout) Run(j *Job) error       { time.Sleep(2 * time.Second); return nil }
func (jt *ShutdownWithoutTimeout) Done(j *Job, err error) {}

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

func (jt *ShutdownWithTimeout) Run(j *Job) error       { time.Sleep(10 * time.Second); return nil }
func (jt *ShutdownWithTimeout) Done(j *Job, err error) {}

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
