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

func getMessage(id string) []byte {
	return []byte(fmt.Sprintf(`{"job_id":"test-job-id-%s","job_type":"test-job-type-1","payload":"{\"id\":\"%s\",\"now\":%d}"}`, id, id, time.Now().Unix()))
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
	m.SetConfigWithJSON(singleTopicConfig)
	m.SetNotifyChan(doneCh)
	m.Run()
	source, _ := m.GetSourceByName("queue-1")
	m.RegisterJobType("queue-1", "test-job-type-1", func() Contract {
		return &TestBasic{}
	})

	// Prepare the message and expected job struct
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

	// New handler
	m := New()
	m.SetConfigWithJSON(singleTopicConfig)
	m.SetNotifyChan(doneCh)
	m.Run()
	source, _ := m.GetSourceByName("queue-1")
	returnCh := make(chan string)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Contract {
		return &TestDone{ReturnCh: returnCh}
	})

	// Send the message
	source.Send(getMessage("foo"))
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

	// New handler
	m := New()
	m.SetConfigWithJSON(singleTopicConfig)
	m.SetNotifyChan(doneCh)
	m.Run()
	source, _ := m.GetSourceByName("queue-1")
	returnCh := make(chan string)
	m.RegisterJobType("queue-1", "test-job-type-1", func() Contract {
		return &TestErr{ReturnCh: returnCh}
	})

	// Send the message
	source.Send(getMessage("foo"))
	time.Sleep(1 * time.Millisecond)
	assert.Equal(t, "bar", <-returnCh)
	<-doneCh
}

// ------------------------------------------------------------------

// Test pointer misuse (run)
type TestPointerMisuseRun struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (tj *TestPointerMisuseRun) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload), &tj)
	time.Sleep(300 * time.Millisecond)
	tj.ReturnCh <- tj.ID
	return nil
}
func (tj *TestPointerMisuseRun) Done(j *Job, err error) {}

func TestPointerMisuseRunJob(t *testing.T) {
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
		return &TestPointerMisuseRun{ReturnCh: returnCh}
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

// ------------------------------------------------------------------

// Test pointer misuse (done)
type TestPointerMisuseDone struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (tj *TestPointerMisuseDone) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload), &tj)
	time.Sleep(300 * time.Millisecond)
	return nil
}
func (tj *TestPointerMisuseDone) Done(j *Job, err error) { tj.ReturnCh <- tj.ID }

func TestPointerMisuseDoneJob(t *testing.T) {
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
		return &TestPointerMisuseDone{ReturnCh: returnCh}
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

// ------------------------------------------------------------------

// Test pointer misuse (custom)
type TestPointerMisuseCustom struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (tj *TestPointerMisuseCustom) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload), &tj)
	time.Sleep(300 * time.Millisecond)
	tj.Custom()
	return nil
}
func (tj *TestPointerMisuseCustom) Done(j *Job, err error) {}
func (tj *TestPointerMisuseCustom) Custom()                { tj.ReturnCh <- tj.ID }

func TestPointerMisuseCustomJob(t *testing.T) {
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
		return &TestPointerMisuseCustom{ReturnCh: returnCh}
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

// ------------------------------------------------------------------

// Test pointer misuse (custom)
type TestPointerMisuseDoneCustom struct {
	ID       string `json:"id"`
	ReturnCh chan string
}

func (tj *TestPointerMisuseDoneCustom) Run(j *Job) error {
	json.Unmarshal([]byte(j.Desc.Payload), &tj)
	time.Sleep(300 * time.Millisecond)
	return nil
}
func (tj *TestPointerMisuseDoneCustom) Done(j *Job, err error) {
	tj.ID = tj.ID + "/done"
	tj.Custom()
}
func (tj *TestPointerMisuseDoneCustom) Custom() { tj.ReturnCh <- tj.ID }

func TestPointerMisuseDoneCustomJob(t *testing.T) {
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
		return &TestPointerMisuseDoneCustom{ReturnCh: returnCh}
	})

	// Expected ID
	expectedID1 := "foo"
	expectedID2 := "bar"

	// Send the messages separately with 150ms delay
	source.Send(getMessage(expectedID1))
	time.Sleep(150 * time.Millisecond)
	source.Send(getMessage(expectedID2))

	assert.Equal(t, expectedID1+"/done", <-returnCh)
	assert.Equal(t, expectedID2+"/done", <-returnCh)
	<-doneCh
	<-doneCh
}

// ------------------------------------------------------------------

// Test 50k jobs
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
		return &TestBasic{}
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
}

// ------------------------------------------------------------------
