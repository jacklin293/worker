package worker

import (
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
	// TODO Log *logrus.Entry
}

type Descriptor struct {
	// UUID V4
	ID string `json:"task_id"`

	// Job name
	// e.g. transactions_backup
	Name string `json:"name"` // e.g. curl

	// Task body of task
	// TODO map[string]interface{} or string
	Payload string `json:"payload"`

	// TODO
	RetryTimes int `json:"retry_times"`

	// Where does this task come frome
	// e.g. api:10.1.2.219
	From string `json:"from"`

	// When does this task being enqueued
	// format: RFC3339
	// e.g. 2017-02-14T06:59:21Z
	EnqueuedAt time.Time `json:"enqueued_at"`

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
		j.Fail(j, err)
	} else {
		j.Succeed(j)
	}
	j.Done(j)
	j.DoneAt = time.Now()
	j.done()
}

func (j *Job) done() {
	j.DoneChan <- j
}
