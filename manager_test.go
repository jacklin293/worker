package worker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWorker(t *testing.T) {
	// New manager
	m := new(Manager)
	m.Topics = []string{"test-topic"}
	m.Init()

	// Initialise job
	var job Job
	job.Topic = "test-topic"
	job.Desc.Type = "test-job"
	job.Do = do
	m.Register(&job)

	// Enqueue a message
	msg, _ := json.Marshal(map[string]interface{}{
		"id":      "ID-001",
		"type":    "test-job",
		"payload": "Hello World",
	})

	// FIXME Enqueue
	Queue <- string(msg)
	time.Sleep(1 * time.Second)

	// FIXME How do I know whether it succeeds?
	assert.Equal(t, "dd", "dd")
}

func do(j *Job) error {
	j.Desc.Payload = "changed"
	return nil
}
