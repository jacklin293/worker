package worker

import (
	"io"
	"time"
)

type Job struct {
	Desc  Descriptor
	Topic string

	Do      func(*Job) error
	Fail    func(*Job, error)
	Succeed func(*Job)
	Done    func(*Job)

	ReceivedAt time.Time
	DidAt      time.Time
	FailedAt   time.Time
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
	// TODO
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
	App interface{}

	// TODO
	SQS struct {
		// e.g. 1483077441
		VisibleAt int64 `json:"visible_at"`
	} `json:"sqs"`
}

func (j *Job) process() {
	defer func(j *Job) {
		if e := recover(); e != nil {
			j.FailedAt = time.Now()
			j.DoneChan <- j
		}
	}(j)
	j.DidAt = time.Now()
	err := j.Do(j)
	if err != nil {
		j.FailedAt = time.Now()
		// FIXME ? if j.Fail != nil
		if j.Fail != nil {
			j.Fail(j, err)
		}
	} else {
		if j.Succeed != nil {
			j.Succeed(j)
		}
	}
	if j.Done != nil {
		j.Done(j)
	}
	j.DoneAt = time.Now()
	j.done()
}

func (j *Job) done() {
	j.DoneChan <- j
}
