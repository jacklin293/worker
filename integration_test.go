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

// Test Do
type Do struct{ returnCh chan string }

func (jt *Do) Do(m Messenger) error        { jt.returnCh <- m.Payload().(string); return nil }
func (jt *Do) Done(m Messenger, err error) {}

func TestDo(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &Do{returnCh: returnCh} })
	q, _ := h.Queue("queue-1")
	go h.Run()
	q.Send(getMessage("id-1", "test-job-type-1", "foo"))
	assert.Equal(t, "foo", <-returnCh)
}

// ------------------------------------------------------------------

// Test Done
type Done struct {
	result   string
	returnCh chan string
}

func (jt *Done) Do(m Messenger) error        { jt.result = m.Payload().(string); return nil }
func (jt *Done) Done(m Messenger, err error) { jt.returnCh <- jt.result }

func TestDone(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &Done{returnCh: returnCh} })
	q, _ := h.Queue("queue-1")
	go h.Run()

	q.Send(getMessage("id-1", "test-job-type-1", "foo"))
	assert.Equal(t, "foo", <-returnCh)
}

// ------------------------------------------------------------------

// Test error
type Err struct{ returnCh chan string }

func (jt *Err) Do(m Messenger) error        { return errors.New("error") }
func (jt *Err) Done(m Messenger, err error) { jt.returnCh <- err.Error() }

func TestErr(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &Err{returnCh: returnCh} })
	q, _ := h.Queue("queue-1")
	go h.Run()

	q.Send(getMessage("id-1", "test-job-type-1", "foo"))
	assert.Equal(t, "error", <-returnCh)
}

// ------------------------------------------------------------------

type JobStructDo struct{ returnCh chan interface{} }

func (jt *JobStructDo) Do(m Messenger) error {
	jt.returnCh <- m.Id()
	jt.returnCh <- m.Type()
	jt.returnCh <- m.Payload()
	jt.returnCh <- m.ReceivedAt().IsZero()
	jt.returnCh <- m.DoneAt().IsZero()
	jt.returnCh <- m.Duration()
	return nil
}
func (jt *JobStructDo) Done(m Messenger, err error) {}

func TestJobStructDo(t *testing.T) {
	t.Parallel()
	returnCh := make(chan interface{})

	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &JobStructDo{returnCh: returnCh} })
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

func (jt *JobStructDone) Do(m Messenger) error { jt.returnCh <- m.Payload(); return nil }
func (jt *JobStructDone) Done(m Messenger, err error) {
	jt.returnCh <- m.Id()
	jt.returnCh <- m.Type()
	jt.returnCh <- m.Payload()
	jt.returnCh <- m.ReceivedAt().IsZero()
	jt.returnCh <- m.DoneAt().IsZero()
	jt.returnCh <- m.Duration()
}

func TestJobStructDone(t *testing.T) {
	t.Parallel()
	returnCh := make(chan interface{})

	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &JobStructDone{returnCh: returnCh} })
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
	payloadDo := <-returnCh
	assert.Equal(t, job["payload"], payloadDo)

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

// Test pointer of job type (do)
type PointerJobTypeDo struct {
	result   string `json:"result"`
	returnCh chan string
}

func (jt *PointerJobTypeDo) Do(m Messenger) error {
	jt.result = m.Payload().(map[string]interface{})["result"].(string)
	time.Sleep(300 * time.Millisecond)
	jt.returnCh <- jt.result
	return nil
}
func (jt *PointerJobTypeDo) Done(m Messenger, err error) {}

func TestPointerJobTypeDo(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &PointerJobTypeDo{returnCh: returnCh} })
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

func (jt *PointerJobTypeDone) Do(m Messenger) error {
	jt.result = m.Payload().(map[string]interface{})["result"].(string)
	time.Sleep(300 * time.Millisecond)
	return nil
}
func (jt *PointerJobTypeDone) Done(m Messenger, err error) { jt.returnCh <- jt.result }

func TestPointerJobTypeDone(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &PointerJobTypeDone{returnCh: returnCh} })
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

func (jt *PointerJobTypeCustom) Do(m Messenger) error {
	jt.result = m.Payload().(map[string]interface{})["result"].(string)
	time.Sleep(300 * time.Millisecond)
	jt.Custom()
	return nil
}
func (jt *PointerJobTypeCustom) Done(m Messenger, err error) {}
func (jt *PointerJobTypeCustom) Custom()                     { jt.returnCh <- jt.result }

func TestPointerJobTypeCustom(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &PointerJobTypeCustom{returnCh: returnCh} })
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

func (jt *PointerJobTypeDoneCustom) Do(m Messenger) error {
	jt.result = m.Payload().(map[string]interface{})["result"].(string)
	time.Sleep(300 * time.Millisecond)
	return nil
}
func (jt *PointerJobTypeDoneCustom) Done(m Messenger, err error) {
	jt.result = jt.result + "/done"
	jt.Custom()
}
func (jt *PointerJobTypeDoneCustom) Custom() { jt.returnCh <- jt.result }

func TestPointerJobTypeDoneCustom(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &PointerJobTypeDoneCustom{returnCh: returnCh} })
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

// Test pointer of job descriptor (do)
type PointerJobDescriptorDo struct{ returnCh chan string }

func (jt *PointerJobDescriptorDo) Do(m Messenger) error {
	time.Sleep(300 * time.Millisecond)
	jt.returnCh <- m.Payload().(string)
	return nil
}
func (jt *PointerJobDescriptorDo) Done(m Messenger, err error) {}

func TestPointerJobDescriptorDo(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &PointerJobDescriptorDo{returnCh: returnCh} })
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

func (jt *PointerJobDescriptorDone) Do(m Messenger) error {
	time.Sleep(300 * time.Millisecond)
	return nil
}
func (jt *PointerJobDescriptorDone) Done(m Messenger, err error) { jt.returnCh <- m.Payload().(string) }

func TestPointerJobDescriptorDone(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &PointerJobDescriptorDone{returnCh: returnCh} })
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

func (jt *Q1J1JobType) Do(m Messenger) error        { jt.returnCh <- m.Payload().(string); return nil }
func (jt *Q1J1JobType) Done(m Messenger, err error) {}

// Test 1 queue and 1 job type
func Test1Queue1JobType(t *testing.T) {
	t.Parallel()
	returnCh := make(chan string)

	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &Q1J1JobType{returnCh: returnCh} })
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

func (jt *Q1J2JobType1) Do(m Messenger) error {
	time.Sleep(300 * time.Millisecond)
	jt.returnCh <- m.Payload().(string)
	return nil
}
func (jt *Q1J2JobType1) Done(m Messenger, err error) {}

type Q1J2JobType2 struct{ returnCh chan string }

func (jt *Q1J2JobType2) Do(m Messenger) error {
	time.Sleep(300 * time.Millisecond)
	jt.returnCh <- m.Payload().(string)
	return nil
}
func (jt *Q1J2JobType2) Done(m Messenger, err error) {}

func Test1Queue2JobTypes(t *testing.T) {
	t.Parallel()
	returnCh1 := make(chan string)
	returnCh2 := make(chan string)

	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &Q1J2JobType1{returnCh: returnCh1} })
	h.RegisterJobType("queue-1", "test-job-type-2", func() Job { return &Q1J2JobType2{returnCh: returnCh2} })
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

func (jt *Q2J1JobType) Do(m Messenger) error {
	time.Sleep(300 * time.Millisecond)
	jt.returnCh <- m.Payload().(string)
	return nil
}

func (jt *Q2J1JobType) Done(m Messenger, err error) {}

func Test2Queues1JobType(t *testing.T) {
	t.Parallel()
	returnCh1 := make(chan string)
	returnCh2 := make(chan string)

	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &Q2J1JobType{returnCh: returnCh1} })
	h.RegisterJobType("queue-2", "test-job-type-1", func() Job { return &Q2J1JobType{returnCh: returnCh2} })
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

func (jt *Q2J2JobType1) Do(m Messenger) error {
	time.Sleep(300 * time.Millisecond)
	jt.returnCh <- m.Payload().(string)
	return nil
}

func (jt *Q2J2JobType1) Done(m Messenger, err error) {}

type Q2J2JobType2 struct{ returnCh chan string }

func (jt *Q2J2JobType2) Do(m Messenger) error {
	time.Sleep(300 * time.Millisecond)
	jt.returnCh <- m.Payload().(string)
	return nil
}

func (jt *Q2J2JobType2) Done(m Messenger, err error) {}

func Test2Queues2JobTypes(t *testing.T) {
	t.Parallel()
	returnCh1 := make(chan string)
	returnCh2 := make(chan string)

	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &Q2J2JobType1{returnCh: returnCh1} })
	h.RegisterJobType("queue-2", "test-job-type-2", func() Job { return &Q2J2JobType2{returnCh: returnCh2} })
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

// Test panic in Do
type PanicDo struct{}

func (jt *PanicDo) Do(m Messenger) error        { panic("panic in Do()"); return nil }
func (jt *PanicDo) Done(m Messenger, err error) {}

func TestPanicDo(t *testing.T) {
	t.Parallel()
	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &PanicDo{} })
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

func (jt *PanicDone) Do(m Messenger) error        { return nil }
func (jt *PanicDone) Done(m Messenger, err error) { panic("panic in Done()") }

func TestPanicDone(t *testing.T) {
	t.Parallel()
	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &PanicDone{} })
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

func (jt *PanicCustom) Do(m Messenger) error        { return nil }
func (jt *PanicCustom) Done(m Messenger, err error) { jt.Custom() }
func (jt *PanicCustom) Custom()                     { panic("panic in Custom()") }

func TestPanicCustom(t *testing.T) {
	t.Parallel()
	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &PanicCustom{} })
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

func (jt *GoChannel100Jobs) Do(m Messenger) error        { return nil }
func (jt *GoChannel100Jobs) Done(m Messenger, err error) {}

func TestGoChannel100Jobs(t *testing.T) {
	t.Parallel()
	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &GoChannel100Jobs{} })
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

func (jt *Sqs100Jobs) Do(m Messenger) error        { return nil }
func (jt *Sqs100Jobs) Done(m Messenger, err error) {}

func TestSqs100Jobs(t *testing.T) {
	h := New()
	h.SetConfig(sqsConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &Sqs100Jobs{} })
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

func (jt *SyncWaitGroup) Do(m Messenger) error        { return nil }
func (jt *SyncWaitGroup) Done(m Messenger, err error) {}

func TestSyncWaitGroup(t *testing.T) {
	t.Parallel()
	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &SyncWaitGroup{} })
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

func (jt *GracefulShutdown) Do(m Messenger) error        { time.Sleep(2 * time.Second); return nil }
func (jt *GracefulShutdown) Done(m Messenger, err error) {}

func TestGracefulShutdown(t *testing.T) {
	t.Parallel()
	h := New()
	h.SetConfig(goChannelConfig)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &GracefulShutdown{} })
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
	assert.WithinDuration(t, start, end, 2200*time.Millisecond) // 2s Jobing time + 100ms sleep + 100ms launching time
	assert.Equal(t, int64(3), h.JobDoneCounter())
}

// ------------------------------------------------------------------

type ShutdownWithTimeout struct{}

func (jt *ShutdownWithTimeout) Do(m Messenger) error        { time.Sleep(10 * time.Second); return nil }
func (jt *ShutdownWithTimeout) Done(m Messenger, err error) {}

func TestShutdownWithTimeout(t *testing.T) {
	t.Parallel()
	h := New()
	h.SetConfig(goChannelConfigWithTimeout)
	h.RegisterJobType("queue-1", "test-job-type-1", func() Job { return &ShutdownWithTimeout{} })
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
