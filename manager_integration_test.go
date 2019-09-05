// +build integration

package worker

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

type TestJob struct {
	Now int64
}

// Test register + getJobTypes

func TestWorker(t *testing.T) {
	conf := `{"env":{"env":"dev"},"containers":[{"name":"queue-1","provider":"gochannel","endpoint":"","source":"","concurrency":3,"enabled":true}]}`
	msg := fmt.Sprintf(`{"job_id":"test-job-id-1","job_type":"test-job-type-1","payload":"{\"now\":%d}"}`, time.Now().Unix())

	// New manager
	m := New()
	m.SetConfigWithJSON(conf)
	m.Run()

	// Initialise job
	var j = TestJob{}
	m.InitJobType(j, "queue-1", "test-job-type-1")

	// Enqueue
	Queue <- msg
	time.Sleep(1 * time.Second)
}

// Test Race condition
func (tj TestJob) Run(j *Job) {
	// fmt.Printf("JobID: %s, JobPayload: '%s', now: %d\n", j.Desc.JobID, j.Desc.Payload, tj.Now)
	tj.Now = time.Now().Unix()
	json.Unmarshal([]byte(j.Desc.Payload), &tj)
	fmt.Println(tj)
}
