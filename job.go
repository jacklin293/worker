package worker

import (
	"fmt"
	"time"
)

type Runner interface {
	Run(*Job)
}

type Job struct {
	Desc Descriptor

	// TODO
	// Log *io.Writer

	doneChan chan *Job

	receivedAt  time.Time
	didAt       time.Time
	doneAt      time.Time
	elapsedTime time.Duration

	// TODO [Optional] SQS message description
	// sqs struct {
	//	MessageID     string
	//	MD5OfBody     string
	//	ReceiptHandle string
	// }
}

type Descriptor struct {
	// UUID V4
	JobID string `json:"job_id"`

	// Job type
	// e.g. transactions_backup
	JobType string `json:"job_type"` // e.g. curl

	// Task body of task
	// TODO map[string]interface{} or string
	Payload string `json:"payload"`

	// TODO
	Timestamp int64 `json:"timestamp"`

	// TODO
	RetryTimes int `json:"retry_times"`

	// TODO
	SQS struct {
		// e.g. 1483077441
		VisibleAt int64 `json:"visible_at"`
	} `json:"sqs"`
}

func (j *Job) validate() (err error) {
	if j.Desc.JobID == "" {
		return fmt.Errorf("Job ID cannot be empty")
	}
	if j.Desc.JobType == "" {
		return fmt.Errorf("Job type cannot be empty")
	}
	if j.Desc.Payload == "" {
		return fmt.Errorf("Payload cannot be empty")
	}
	return
}

func (j *Job) process(jb Runner) {
	defer func(j *Job) {
		if e := recover(); e != nil {
			j.doneChan <- j
		}
	}(j)
	j.didAt = time.Now()
	jb.Run(j)
	j.done()
	j.doneChan <- j
}

func (j *Job) done() {
	j.doneAt = time.Now()
	j.elapsedTime = j.doneAt.Sub(j.receivedAt)
}
