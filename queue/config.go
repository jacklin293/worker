package queue

import (
	"errors"
	"fmt"
)

type Configure interface {
	validate() error
	New() (Queuer, error)
}

type Config struct {
	// Required
	Name              string `json:"name"`
	QueueType         string `json:"queue_type"`
	QueueConcurrency  int64  `json:"queue_concurrency"`
	WorkerConcurrency int64  `json:"worker_concurrency"`
	Enabled           bool   `json:"enabled"`

	sqsConfig       `json:"sqs"`
	goChannelConfig `json:"go_channel"`
}

func (c *Config) Validate() (err error) {
	// TODO only contain a-z, A-Z, -, _   unique name
	if c.Name == "" {
		return errors.New("name cannot be empty")
	}
	if c.QueueConcurrency == 0 {
		return fmt.Errorf("queue_concurrency cannot be 0")
	}
	if c.WorkerConcurrency == 0 {
		return fmt.Errorf("worker_concurrency cannot be 0")
	}

	// Validate queue_type
	switch c.QueueType {
	case "sqs", "go_channel":
	default:
		err = fmt.Errorf("queue_type '%s' not supported", c.QueueType)
	}
	return
}

// Convert config into Configure
func (c *Config) GetQueueAttr() Configure {
	switch c.QueueType {
	case "sqs":
		return &c.sqsConfig
	case "go_channel":
		return &c.goChannelConfig
	}
	return nil
}