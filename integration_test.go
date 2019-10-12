// +build integration

package worker

import (
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var goChannelConfig = `
{
	"log_enabled": false,
	"queues": [
		{
			"name":"queue-1",
			"queue_type":"go_channel",
			"queue_concurrency": 3,
			"worker_concurrency":5,
			"enabled":true,
			"go_channel": {
				"size": 0
			}
		},
		{
			"name":"queue-2",
			"queue_type":"go_channel",
			"queue_concurrency": 3,
			"worker_concurrency":5,
			"enabled":true,
			"go_channel": {
				"size": 0
			}
		}
	]
}`

var goChannelConfigWithTimeout = `
{
	"log_enabled": false,
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
	"log_enabled": false,
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

func getMapPaylaod(text string) map[string]interface{} {
	m := map[string]interface{}{}
	m["result"] = text
	m["timestamp"] = time.Now().UnixNano()
	return m
}

func getMessage(id string, jobType string, payload interface{}) []byte {
	m := map[string]interface{}{}
	m["id"] = id
	m["type"] = jobType
	m["payload"] = payload
	json, _ := json.Marshal(m)
	return json
}

// ------------------------------------------------------------------

// Test run
type Run struct{ returnCh chan string }

func (jt *Run) Run(j *Job) error       { jt.returnCh <- j.Payload().(string); return nil }
func (jt *Run) Done(j *Job, err error) {}

func TestRun(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &Run{returnCh: returnCh} })
	q, _ := h.Queue("queue-1")
	go h.Run()
	q.Send(getMessage("id-1", "test-job-type-1", "foo"))
	assert.Equal(t, "foo", <-returnCh)
}

// ------------------------------------------------------------------

// Test done
type Done struct {
	result   string
	returnCh chan string
}

func (t *Done) Run(j *Job) error       { t.result = j.Payload().(string); return nil }
func (t *Done) Done(j *Job, err error) { t.returnCh <- t.result }

func TestDone(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &Done{returnCh: returnCh} })
	q, _ := h.Queue("queue-1")
	go h.Run()

	q.Send(getMessage("id-1", "test-job-type-1", "foo"))
	assert.Equal(t, "foo", <-returnCh)
}

// ------------------------------------------------------------------

// Test err
type Err struct{ returnCh chan string }

func (t *Err) Run(j *Job) error       { return errors.New("error") }
func (t *Err) Done(j *Job, err error) { t.returnCh <- err.Error() }

func TestErr(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &Err{returnCh: returnCh} })
	q, _ := h.Queue("queue-1")
	go h.Run()

	q.Send(getMessage("id-1", "test-job-type-1", "foo"))
	assert.Equal(t, "error", <-returnCh)
}

// ------------------------------------------------------------------

type JobStructRun struct{ returnCh chan interface{} }

func (jt *JobStructRun) Run(j *Job) error {
	jt.returnCh <- j.Id()
	jt.returnCh <- j.Type()
	jt.returnCh <- j.Payload()
	jt.returnCh <- j.ReceivedAt().IsZero()
	jt.returnCh <- j.DoneAt().IsZero()
	jt.returnCh <- j.Duration()
	return nil
}
func (jt *JobStructRun) Done(j *Job, err error) {}

func TestJobStructRun(t *testing.T) {
	t.Parallel()
	returnCh := make(chan interface{})

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &JobStructRun{returnCh: returnCh} })
	q, err := h.Queue("queue-1")
	if err != nil {
		t.Log(err)
		return
	}
	go h.Run()
	q.Send(getMessage("id-1", "test-job-type-1", "foo"))
	id := <-returnCh
	assert.Equal(t, "id-1", id.(string))
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

type JobStructDone struct{ returnCh chan interface{} }

func (jt *JobStructDone) Run(j *Job) error { jt.returnCh <- j.Payload(); return nil }
func (jt *JobStructDone) Done(j *Job, err error) {
	jt.returnCh <- j.Id()
	jt.returnCh <- j.Type()
	jt.returnCh <- j.Payload()
	jt.returnCh <- j.ReceivedAt().IsZero()
	jt.returnCh <- j.DoneAt().IsZero()
	jt.returnCh <- j.Duration()
}

func TestJobStructDone(t *testing.T) {
	t.Parallel()
	returnCh := make(chan interface{})

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &JobStructDone{returnCh: returnCh} })
	q, err := h.Queue("queue-1")
	if err != nil {
		t.Log(err)
		return
	}
	go h.Run()
	payload := getMessage("id-1", "test-job-type-1", getMapPaylaod("foo"))
	q.Send(payload)

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

// Test pointer of job type (run)
type PointerJobTypeRun struct {
	result   string `json:"result"`
	returnCh chan string
}

func (jt *PointerJobTypeRun) Run(j *Job) error {
	jt.result = j.Payload().(map[string]interface{})["result"].(string)
	time.Sleep(300 * time.Millisecond)
	jt.returnCh <- jt.result
	return nil
}
func (jt *PointerJobTypeRun) Done(j *Job, err error) {}

func TestPointerJobTypeRun(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PointerJobTypeRun{returnCh: returnCh} })
	q, _ := h.Queue("queue-1")
	go h.Run()

	expected1 := "foo"
	expected2 := "bar"
	q.Send(getMessage("id-1", "test-job-type-1", getMapPaylaod(expected1)))
	time.Sleep(150 * time.Millisecond)
	q.Send(getMessage("id-2", "test-job-type-1", getMapPaylaod(expected2)))
	assert.Equal(t, expected1, <-returnCh)
	assert.Equal(t, expected2, <-returnCh)
}

// ------------------------------------------------------------------

// Test pointer of job type (done)
type PointerJobTypeDone struct {
	result   string `json:"result"`
	returnCh chan string
}

func (jt *PointerJobTypeDone) Run(j *Job) error {
	jt.result = j.Payload().(map[string]interface{})["result"].(string)
	time.Sleep(300 * time.Millisecond)
	return nil
}
func (jt *PointerJobTypeDone) Done(j *Job, err error) { jt.returnCh <- jt.result }

func TestPointerJobTypeDone(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PointerJobTypeDone{returnCh: returnCh} })
	q, _ := h.Queue("queue-1")
	go h.Run()

	expected1 := "foo"
	expected2 := "bar"
	q.Send(getMessage("id-1", "test-job-type-1", getMapPaylaod(expected1)))
	time.Sleep(150 * time.Millisecond)
	q.Send(getMessage("id-2", "test-job-type-1", getMapPaylaod(expected2)))
	assert.Equal(t, expected1, <-returnCh)
	assert.Equal(t, expected2, <-returnCh)
}

// ------------------------------------------------------------------

// Test pointer of job struct (custom)
type PointerJobTypeCustom struct {
	result   string `json:"result"`
	returnCh chan string
}

func (jt *PointerJobTypeCustom) Run(j *Job) error {
	jt.result = j.Payload().(map[string]interface{})["result"].(string)
	time.Sleep(300 * time.Millisecond)
	jt.Custom()
	return nil
}
func (jt *PointerJobTypeCustom) Done(j *Job, err error) {}
func (jt *PointerJobTypeCustom) Custom()                { jt.returnCh <- jt.result }

func TestPointerJobTypeCustom(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PointerJobTypeCustom{returnCh: returnCh} })
	q, _ := h.Queue("queue-1")
	go h.Run()

	expected1 := "foo"
	expected2 := "bar"
	q.Send(getMessage("id-1", "test-job-type-1", getMapPaylaod(expected1)))
	time.Sleep(150 * time.Millisecond)
	q.Send(getMessage("id-2", "test-job-type-1", getMapPaylaod(expected2)))
	assert.Equal(t, expected1, <-returnCh)
	assert.Equal(t, expected2, <-returnCh)
}

// ------------------------------------------------------------------

// Test pointer of job struct (done->custom)
type PointerJobTypeDoneCustom struct {
	result   string `json:"result"`
	returnCh chan string
}

func (jt *PointerJobTypeDoneCustom) Run(j *Job) error {
	jt.result = j.Payload().(map[string]interface{})["result"].(string)
	time.Sleep(300 * time.Millisecond)
	return nil
}
func (jt *PointerJobTypeDoneCustom) Done(j *Job, err error) {
	jt.result = jt.result + "/done"
	jt.Custom()
}
func (jt *PointerJobTypeDoneCustom) Custom() { jt.returnCh <- jt.result }

func TestPointerJobTypeDoneCustom(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PointerJobTypeDoneCustom{returnCh: returnCh} })
	q, _ := h.Queue("queue-1")
	go h.Run()

	expected1 := "foo"
	expected2 := "bar"
	q.Send(getMessage("id-1", "test-job-type-1", getMapPaylaod(expected1)))
	time.Sleep(150 * time.Millisecond)
	q.Send(getMessage("id-2", "test-job-type-1", getMapPaylaod(expected2)))
	assert.Equal(t, expected1+"/done", <-returnCh)
	assert.Equal(t, expected2+"/done", <-returnCh)
}

// ------------------------------------------------------------------

// Test pointer of job descriptor (run)
type PointerJobDescriptorRun struct{ returnCh chan string }

func (jt *PointerJobDescriptorRun) Run(j *Job) error {
	time.Sleep(300 * time.Millisecond)
	jt.returnCh <- j.Payload().(string)
	return nil
}
func (jt *PointerJobDescriptorRun) Done(j *Job, err error) {}

func TestPointerJobDescriptorRun(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PointerJobDescriptorRun{returnCh: returnCh} })
	q, _ := h.Queue("queue-1")
	go h.Run()

	expected1 := "foo"
	expected2 := "bar"
	q.Send(getMessage("id-1", "test-job-type-1", expected1))
	time.Sleep(150 * time.Millisecond)
	q.Send(getMessage("id-2", "test-job-type-1", expected2))
	assert.Equal(t, expected1, <-returnCh)
	assert.Equal(t, expected2, <-returnCh)
}

// ------------------------------------------------------------------

// Test pointer of job descriptor (done)
type PointerJobDescriptorDone struct{ returnCh chan string }

func (jt *PointerJobDescriptorDone) Run(j *Job) error       { time.Sleep(300 * time.Millisecond); return nil }
func (jt *PointerJobDescriptorDone) Done(j *Job, err error) { jt.returnCh <- j.Payload().(string) }

func TestPointerJobDescriptorDone(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &PointerJobDescriptorDone{returnCh: returnCh} })
	q, _ := h.Queue("queue-1")
	go h.Run()

	expected1 := "foo"
	expected2 := "bar"
	q.Send(getMessage("id-1", "test-job-type-1", expected1))
	time.Sleep(150 * time.Millisecond)
	q.Send(getMessage("id-2", "test-job-type-1", expected2))
	assert.Equal(t, expected1, <-returnCh)
	assert.Equal(t, expected2, <-returnCh)
}

// ------------------------------------------------------------------

type Q1J1JobType struct{ returnCh chan string }

func (jt *Q1J1JobType) Run(j *Job) error       { jt.returnCh <- j.Payload().(string); return nil }
func (jt *Q1J1JobType) Done(j *Job, err error) {}

// Test 1 queue and 1 job type
func Test1Queue1JobType(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &Q1J1JobType{returnCh: returnCh} })
	q, _ := h.Queue("queue-1")
	go h.Run()

	expected1 := "foo"
	expected2 := "bar"
	q.Send(getMessage("id-1", "test-job-type-1", expected1))
	time.Sleep(150 * time.Millisecond)
	q.Send(getMessage("id-2", "test-job-type-1", expected2))
	assert.Equal(t, expected1, <-returnCh)
	assert.Equal(t, expected2, <-returnCh)
}

// ------------------------------------------------------------------

// Test 1 queue and 2 job types
type Q1J2JobType1 struct{ returnCh chan string }

func (jt *Q1J2JobType1) Run(j *Job) error {
	time.Sleep(300 * time.Millisecond)
	jt.returnCh <- j.Payload().(string)
	return nil
}
func (jt *Q1J2JobType1) Done(j *Job, err error) {}

type Q1J2JobType2 struct{ returnCh chan string }

func (jt *Q1J2JobType2) Run(j *Job) error {
	time.Sleep(300 * time.Millisecond)
	jt.returnCh <- j.Payload().(string)
	return nil
}
func (jt *Q1J2JobType2) Done(j *Job, err error) {}

func Test1Queue2JobTypes(t *testing.T) {
	t.Parallel()
	returnCh1 := make(chan string)
	returnCh2 := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &Q1J2JobType1{returnCh: returnCh1} })
	h.RegisterJobType("queue-1", "test-job-type-2", func() Process { return &Q1J2JobType2{returnCh: returnCh2} })
	q, _ := h.Queue("queue-1")
	go h.Run()

	expected1 := "foo"
	expected2 := "bar"
	q.Send(getMessage("id-1", "test-job-type-1", expected1))
	time.Sleep(150 * time.Millisecond)
	q.Send(getMessage("id-2", "test-job-type-2", expected2))
	assert.Equal(t, expected1, <-returnCh1)
	assert.Equal(t, expected2, <-returnCh2)
}

// ------------------------------------------------------------------

// Test 2 queues and 1 job type
type Q2J1JobType struct{ returnCh chan string }

func (jt *Q2J1JobType) Run(j *Job) error {
	time.Sleep(300 * time.Millisecond)
	jt.returnCh <- j.Payload().(string)
	return nil
}

func (jt *Q2J1JobType) Done(j *Job, err error) {}

func Test2Queues1JobType(t *testing.T) {
	t.Parallel()
	returnCh1 := make(chan string)
	returnCh2 := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &Q2J1JobType{returnCh: returnCh1} })
	h.RegisterJobType("queue-2", "test-job-type-1", func() Process { return &Q2J1JobType{returnCh: returnCh2} })
	q1, _ := h.Queue("queue-1")
	q2, _ := h.Queue("queue-2")
	go h.Run()

	expected1 := "foo"
	expected2 := "bar"
	q1.Send(getMessage("id-1", "test-job-type-1", expected1))
	time.Sleep(150 * time.Millisecond)
	q2.Send(getMessage("id-2", "test-job-type-1", expected2))
	assert.Equal(t, expected1, <-returnCh1)
	assert.Equal(t, expected2, <-returnCh2)
}

// ------------------------------------------------------------------

// Test 2 queues and 2 job type
type Q2J2JobType1 struct{ returnCh chan string }

func (jt *Q2J2JobType1) Run(j *Job) error {
	time.Sleep(300 * time.Millisecond)
	jt.returnCh <- j.Payload().(string)
	return nil
}

func (jt *Q2J2JobType1) Done(j *Job, err error) {}

type Q2J2JobType2 struct{ returnCh chan string }

func (jt *Q2J2JobType2) Run(j *Job) error {
	time.Sleep(300 * time.Millisecond)
	jt.returnCh <- j.Payload().(string)
	return nil
}

func (jt *Q2J2JobType2) Done(j *Job, err error) {}

func Test2Queues2JobTypes(t *testing.T) {
	t.Parallel()
	returnCh1 := make(chan string)
	returnCh2 := make(chan string)

	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &Q2J2JobType1{returnCh: returnCh1} })
	h.RegisterJobType("queue-2", "test-job-type-2", func() Process { return &Q2J2JobType2{returnCh: returnCh2} })
	q1, _ := h.Queue("queue-1")
	q2, _ := h.Queue("queue-2")
	go h.Run()

	expected1 := "foo"
	expected2 := "bar"
	q1.Send(getMessage("id-1", "test-job-type-1", expected1))
	time.Sleep(150 * time.Millisecond)
	q2.Send(getMessage("id-2", "test-job-type-2", expected2))
	assert.Equal(t, expected1, <-returnCh1)
	assert.Equal(t, expected2, <-returnCh2)
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
	q, _ := h.Queue("queue-1")
	go func() {
		q.Send(getMessage("id-1", "test-job-type-1", "foo"))
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
	q, _ := h.Queue("queue-1")
	go func() {
		q.Send(getMessage("id-1", "test-job-type-1", "foo"))
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
	q, _ := h.Queue("queue-1")
	go func() {
		q.Send(getMessage("id-1", "test-job-type-1", "foo"))
		time.Sleep(100 * time.Millisecond) // Give the worker some time to receive the message.
		h.Shutdown()
	}()
	h.Run()
	assert.Equal(t, int64(1), h.JobDoneCounter())
}

// ------------------------------------------------------------------

// Test go_channel
type GoChannel100Jobs struct{}

func (jt *GoChannel100Jobs) Run(j *Job) error       { return nil }
func (jt *GoChannel100Jobs) Done(j *Job, err error) {}

func TestGoChannel100Jobs(t *testing.T) {
	t.Parallel()
	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &GoChannel100Jobs{} })
	q, _ := h.Queue("queue-1")
	total := int64(100)

	go func() {
		for i := int64(0); i < total; i++ {
			q.Send(getMessage("id-1", "test-job-type-1", strconv.FormatInt(i, 10)))
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
type Sqs100Jobs struct{}

func (jt *Sqs100Jobs) Run(j *Job) error       { return nil }
func (jt *Sqs100Jobs) Done(j *Job, err error) {}

func TestSqs100Jobs(t *testing.T) {
	h := New()
	h.InitWithJsonConfig(sqsConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &Sqs100Jobs{} })
	q, _ := h.Queue("queue-1")

	total := int64(100)
	go func() {
		for i := int64(0); i < total; i++ {
			q.Send([][]byte{getMessage("id-1", "test-job-type-1", strconv.FormatInt(i, 10))})
		}
		time.Sleep(100 * time.Millisecond) // Give the worker some time to receive the message.
		h.Shutdown()
	}()
	h.Run()
	assert.Equal(t, total, h.JobDoneCounter())
	t.Logf("counter/total: %d/%d\n", h.JobDoneCounter(), total)
}

// ------------------------------------------------------------------

// Test sync.WaitGroup with 50k jobs
type SyncWaitGroup struct{}

func (jt *SyncWaitGroup) Run(j *Job) error       { return nil }
func (jt *SyncWaitGroup) Done(j *Job, err error) {}

func TestSyncWaitGroup(t *testing.T) {
	t.Parallel()
	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &SyncWaitGroup{} })
	q, _ := h.Queue("queue-1")

	total := int64(50000)
	go func(total int64) {
		for i := int64(0); i < total; i++ {
			q.Send(getMessage("id-1", "test-job-type-1", strconv.FormatInt(i, 10)))
		}
		time.Sleep(100 * time.Millisecond) // Give the worker some time to receive the message.
		h.Shutdown()
	}(total)
	h.Run()
	assert.Equal(t, total, h.JobDoneCounter())
	t.Logf("counter/total: %d/%d\n", h.JobDoneCounter(), total)
}

// ------------------------------------------------------------------

type GracefulShutdown struct{}

func (jt *GracefulShutdown) Run(j *Job) error       { time.Sleep(2 * time.Second); return nil }
func (jt *GracefulShutdown) Done(j *Job, err error) {}

func TestGracefulShutdown(t *testing.T) {
	t.Parallel()
	h := New()
	h.InitWithJsonConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Process { return &GracefulShutdown{} })
	q, _ := h.Queue("queue-1")

	total := int64(3)
	go func(total int64) {
		for i := int64(0); i < total; i++ {
			q.Send(getMessage("id-1", "test-job-type-1", strconv.FormatInt(i, 10)))
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
	q, _ := h.Queue("queue-1")

	total := int64(3)
	go func(total int64) {
		for i := int64(0); i < total; i++ {
			q.Send(getMessage("id-1", "test-job-type-1", strconv.FormatInt(i, 10)))
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
