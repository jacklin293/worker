package worker

import (
	"io"
	"time"
)

type JobBehaviour interface {
	Do(*Job)
}

type Job struct {
	Desc  Descriptor
	Topic string

	ReceivedAt time.Time
	DidAt      time.Time
	DoneAt     time.Time
	Duration   time.Duration

	// SQS message description
	SQS struct {
		MessageID     string
		MD5OfBody     string
		ReceiptHandle string
	}

	DoneChan chan *Job

	// TODO Config
	Log *io.Writer
}

type Descriptor struct {
	// UUID V4
	ID string `json:"id"`

	// Job type
	// e.g. transactions_backup
	Type string `json:"type"` // e.g. curl

	// Task body of task
	// TODO map[string]interface{} or string
	Payload string `json:"payload"`

	// TODO
	RetryTimes int `json:"retry_times"`

	// TODO
	SQS struct {
		// e.g. 1483077441
		VisibleAt int64 `json:"visible_at"`
	} `json:"sqs"`
}

func (j *Job) process(jb JobBehaviour) {
	defer func(j *Job) {
		if e := recover(); e != nil {
			j.DoneChan <- j
		}
	}(j)
	j.DidAt = time.Now()
	jb.Do(j)
	j.DoneAt = time.Now()
	j.done()
}

func (j *Job) done() {
	j.DoneChan <- j
}
