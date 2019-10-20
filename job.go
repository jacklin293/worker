package worker

import (
	"encoding/json"
	"fmt"
	"time"
)

type Job interface {
	Do(Messenger) error
	Done(Messenger, error)
}

type Messenger interface {
	Id() string
	Type() string
	QueueName() string
	Payload() interface{}
	ReceivedAt() time.Time
	DoneAt() time.Time
	Duration() time.Duration
}

type Message struct {
	queueName  string
	descriptor descriptor
	receivedAt time.Time
	doneAt     time.Time
	duration   time.Duration
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

func newMessage(payload []byte) (msg Message, err error) {
	if err = json.Unmarshal(payload, &msg.descriptor); err != nil {
		return
	}
	if err = msg.validate(); err != nil {
		return
	}
	return
}

func (msg *Message) validate() (err error) {
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

func (msg *Message) done(j Job, err error) {
	msg.doneAt = time.Now()
	msg.duration = msg.doneAt.Sub(msg.receivedAt)
	j.Done(msg, err)
}

func (msg *Message) Id() string {
	return msg.descriptor.Id
}

func (msg *Message) Type() string {
	return msg.descriptor.Type
}

func (msg *Message) QueueName() string {
	return msg.queueName
}

func (msg *Message) Payload() interface{} {
	return msg.descriptor.Payload
}

func (msg *Message) ReceivedAt() time.Time {
	return msg.receivedAt
}

func (msg *Message) DoneAt() time.Time {
	return msg.doneAt
}

func (msg *Message) Duration() time.Duration {
	return msg.duration
}
