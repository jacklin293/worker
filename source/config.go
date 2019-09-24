package source

import (
	"errors"
	"fmt"
)

type Configure interface {
	validate() error
	new() Sourcer
}

type Config struct {
	// Required
	Name              string `json:"name"`
	SourceType        string `json:"source_type"`
	SourceConcurrency int64  `json:"source_concurrency"`
	WorkerConcurrency int64  `json:"worker_concurrency"`
	Enabled           bool   `json:"enabled"`

	SourceTypeConfig Configure
	sqsConfig        `json:"sqs"`
	goChannelConfig  `json:"go_channel"`
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
	// Convert config into Configure
	switch c.SourceType {
	case "sqs":
		c.SourceTypeConfig = &c.sqsConfig
	case "go_channel":
		c.SourceTypeConfig = &c.goChannelConfig
	default:
		err = fmt.Errorf("source_type not supported")
	}
	return
}

func (c *Config) New() Sourcer {
	return c.SourceTypeConfig.new()
}
