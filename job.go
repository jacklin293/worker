package worker

import (
	"fmt"
	"time"
)

type Process interface {
	Run(*Job) error
	Done(*Job, error)
}

type jobTypeFunc func() Process

type Job struct {
	descriptor descriptor
	receivedAt time.Time
	doneAt     time.Time
	duration   time.Duration

	doneChan chan *Job
}

type descriptor struct {
	// UUID V4
	Id string `json:"id"`

	// Job type
	// e.g. transactions_backup
	Type string `json:"type"`

	// Task body of task
	Payload interface{} `json:"payload"`
}

func (j *Job) validate() (err error) {
	if j.descriptor.Id == "" {
		return fmt.Errorf("Job Id can't be empty")
	}
	if j.descriptor.Type == "" {
		return fmt.Errorf("Job type can't be empty")
	}
	if j.descriptor.Payload == "" {
		return fmt.Errorf("Payload can't be empty")
	}
	return
}

func (j *Job) process(f jobTypeFunc) {
	jb := f()
	err := jb.Run(j)
	j.done(jb, err)
	j.doneChan <- j
}

func (j *Job) done(jb Process, err error) {
	j.doneAt = time.Now()
	j.duration = j.doneAt.Sub(j.receivedAt)
	jb.Done(j, err)
}

func (j *Job) Id() string {
	return j.descriptor.Id
}

func (j *Job) Type() string {
	return j.descriptor.Type
}

func (j *Job) Payload() interface{} {
	return j.descriptor.Payload
}

func (j *Job) ReceivedAt() time.Time {
	return j.receivedAt
}

func (j *Job) DoneAt() time.Time {
	return j.doneAt
}

func (j *Job) Duration() time.Duration {
	return j.duration
}
