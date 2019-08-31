package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestJob struct {
	Now int64
}

// Test register + getJobTypes

func TestWorker(t *testing.T) {
	// New manager
	topic := Topic{
		Name:         "test-topic-1",
		WorkerNumber: 3,
		Endpoint:     "FAKE",
	}
	m, err := New([]Topic{topic})
	if err != nil {
		log.Fatal(err)
	}

	// Initialise job
	var j JobBehaviour = TestJob{}
	m.Register(j, "test-topic-1", "test-job_type-1")
	m.Register(j, "test-topic-2", "test-job_type-1")
	m.Register(j, "test-topic-2", "test-job_type-2")

	// Enqueue a message

	// FIXME Enqueue
	Queue <- string(newMessage("1"))
	// time.Sleep(3 * time.Second)
	// Queue <- string(newMessage("2"))
	time.Sleep(2 * time.Second)

	// FIXME How do I know whether it succeeds?
	assert.Equal(t, "dd", "dd")
}

func newMessage(id string) string {
	msg, _ := json.Marshal(map[string]interface{}{
		"job_id":   "ID-00" + id,
		"job_type": "test-job_type-1",
		"payload":  fmt.Sprintf("rand num: %d", rand.Intn(100)),
	})
	return string(msg)
}

// Test Race condition
func (tj TestJob) Run(j *Job) {
	// fmt.Printf("JobID: %s, JobPayload: '%s', now: %d\n", j.Desc.JobID, j.Desc.Payload, tj.Now)
	tj.Now = time.Now().Unix()
	j.Desc.Payload = fmt.Sprintf("rand num: %d", rand.Intn(100))
	// fmt.Printf("JobID: %s, JobPayload: '%s', now: %d changed\n", j.Desc.JobID, j.Desc.Payload, tj.Now)
	// fmt.Printf("JobID: %s, ----- sleep for 5s -----\n", j.Desc.JobID)
	time.Sleep(1 * time.Second)
	// fmt.Printf("JobID: %s, JobPayload: '%s', now: %d after 5s\n", j.Desc.JobID, j.Desc.Payload, tj.Now)
	tj.dd(j)
	// fmt.Printf("JobID: %s, JobPayload: '%s', now: %d after dd\n", j.Desc.JobID, j.Desc.Payload, tj.Now)
}

func (tj *TestJob) dd(j *Job) {
	tj.Now = time.Now().Unix()
	j.Desc.Payload = fmt.Sprintf("rand num: %d", rand.Intn(100))
	// fmt.Printf("JobID: %s, JobPayload: '%s', now: %d changed by dd\n", j.Desc.JobID, j.Desc.Payload, tj.Now)
}
