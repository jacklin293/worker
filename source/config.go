package source

import (
	"errors"
	"fmt"
)

type Configure interface {
	validate() error
	New() (Sourcer, error)
}

type Config struct {
	// Required
	Name              string `json:"name"`
	SourceType        string `json:"source_type"`
	SourceConcurrency int64  `json:"source_concurrency"`
	WorkerConcurrency int64  `json:"worker_concurrency"`
	Enabled           bool   `json:"enabled"`

	sourceAttr      Configure
	sqsConfig       `json:"sqs"`
	goChannelConfig `json:"go_channel"`
}

func (c *Config) Validate() (err error) {
	// TODO only contain a-z, A-Z, -, _   unique name
	if c.Name == "" {
		return errors.New("name cannot be empty")
	}
	if c.SourceConcurrency == 0 {
		return fmt.Errorf("source_concurrency cannot be 0")
	}
	if c.WorkerConcurrency == 0 {
		return fmt.Errorf("worker_concurrency cannot be 0")
	}

	// Validate source_type
	switch c.SourceType {
	case "sqs", "go_channel":
	default:
		err = fmt.Errorf("source_type '%s' not supported", c.SourceType)
	}
	return
}

// Convert config into Configure
func (c *Config) GetSourceAttr() Configure {
	switch c.SourceType {
	case "sqs":
		return &c.sqsConfig
	case "go_channel":
		return &c.goChannelConfig
	}
	return nil
}
