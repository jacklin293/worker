package queue

import (
	"fmt"
	"regexp"
)

type ConfigNewer interface {
	validate() error
	New() (QueueContainer, error)
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
	if !regexp.MustCompile(`^[\w-]+$`).MatchString(c.Name) {
		return fmt.Errorf("name '%s' can only contain letter, number, underscore and hyphen", c.Name)
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
		return
	default:
		err = fmt.Errorf("queue_type '%s' not supported", c.QueueType)
	}
	return
}

func (c *Config) QueueAttr() ConfigNewer {
	switch c.QueueType {
	case "sqs":
		return &c.sqsConfig
	case "go_channel":
		return &c.goChannelConfig
	}
	return nil
}
