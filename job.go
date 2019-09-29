package worker

import (
	"fmt"
	"time"
)

type Process interface {
	Run(*Job) error
	Done(*Job, error)
}

type process func() Process

type Job struct {
	Desc Descriptor

	doneChan chan *Job

	receivedAt time.Time
	didAt      time.Time
	doneAt     time.Time
	duration   time.Duration

	// TODO
	// Log *io.Writer
}

type Descriptor struct {
	// UUID V4
	JobID string `json:"job_id"`

	// Job type
	// e.g. transactions_backup
	JobType string `json:"job_type"` // e.g. curl

	// Task body of task
	Payload interface{} `json:"payload"`

	// TODO
	Timestamp int64 `json:"timestamp"`

	// TODO
	RetryCount int64 `json:"retry_count"`

	// TODO
	SQS struct {
		// e.g. 1483077441
		VisibleAt int64 `json:"visible_at"`
	} `json:"sqs"`
}

func (j *Job) validate() (err error) {
	if j.Desc.JobID == "" {
		return fmt.Errorf("Job ID can't be empty")
	}
	if j.Desc.JobType == "" {
		return fmt.Errorf("Job type can't be empty")
	}
	if j.Desc.Payload == "" {
		return fmt.Errorf("Payload can't be empty")
	}
	return
}

func (j *Job) process(p process) {
	j.didAt = time.Now()
	jb := p()
	err := jb.Run(j)
	j.done(jb, err)
	j.doneChan <- j
}

func (j *Job) done(jb Process, err error) {
	j.doneAt = time.Now()
	j.duration = j.doneAt.Sub(j.receivedAt)
	jb.Done(j, err)
}
