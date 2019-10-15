package worker

import (
	"fmt"
	"time"
)

type Job interface {
	Run(Messenger) error
	Done(Messenger, error)
}

type Messenger interface {
	Id() string
	Type() string
	Payload() interface{}
	ReceivedAt() time.Time
	DoneAt() time.Time
	Duration() time.Duration
}

type jobTypeFunc func() Job

type message struct {
	descriptor descriptor
	doneAt     time.Time
	doneChan   chan *message
	duration   time.Duration
	receivedAt time.Time
}

type descriptor struct {
	// UUID V4
	Id string `json:"id"`

	// Task body of task
	Payload interface{} `json:"payload"`

	// Job type
	// e.g. transactions_backup
	Type string `json:"type"`
}

func (msg *message) validate() (err error) {
	if msg.descriptor.Id == "" {
		return fmt.Errorf("Job Id can't be empty")
	}
	if msg.descriptor.Type == "" {
		return fmt.Errorf("Job type can't be empty")
	}
	if msg.descriptor.Payload == "" {
		return fmt.Errorf("Payload can't be empty")
	}
	return
}

func (msg *message) process(f jobTypeFunc) {
	j := f()
	err := j.Run(msg)
	msg.done(j, err)
	msg.doneChan <- msg
}

func (msg *message) done(j Job, err error) {
	msg.doneAt = time.Now()
	msg.duration = msg.doneAt.Sub(msg.receivedAt)
	j.Done(msg, err)
}

func (msg *message) Id() string {
	return msg.descriptor.Id
}

func (msg *message) Type() string {
	return msg.descriptor.Type
}

func (msg *message) Payload() interface{} {
	return msg.descriptor.Payload
}

func (msg *message) ReceivedAt() time.Time {
	return msg.receivedAt
}

func (msg *message) DoneAt() time.Time {
	return msg.doneAt
}

func (msg *message) Duration() time.Duration {
	return msg.duration
}
