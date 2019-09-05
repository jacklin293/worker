// +build integration

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
	conf := `{"env":{"env":"dev"},"containers":[{"name":"queue-1","provider":"gochannel","endpoint":"","source":"","concurrency":3,"enabled":true}]}`

	// New manager
	m, err := New()
	if err != nil {
		log.Fatal(err)
	}
	m.SetConfigWithJSON(conf)
	m.Run()

	// Initialise job
	var j = TestJob{}
	m.NewJobType(j, "queue-1", "test-job_type-1")
	m.NewJobType(j, "queue-1", "test-job_type-1")
	m.NewJobType(j, "queue-1", "test-job_type-2")

	// FIXME Enqueue
	Queue <- newMessage("1")
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
}
