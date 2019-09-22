package source

import (
	"errors"
	"fmt"
)

type Config struct {
	Name        string `json:"name"`
	SourceType  string `json:"source_type"`
	Endpoint    string `json:"endpoint"`
	Topic       string `json:"topic"`
	Concurrency int    `json:"concurrency"`
	Enabled     bool   `json:"enabled"`
	Metadata    struct {
		SQS SqsConfig `json:"sqs"`
	} `json:"metadata"`
}

func (c *Config) Validate() (err error) {
	// TODO switch c.Name {

	// TODO only contain a-z, A-Z, -, _   unique name
	if c.Name == "" {
		return errors.New("name cannot be empty")
	}
	// TODO only contain a-z, A-Z, -, _   unique name
	// TODO only [ "go-channel", "sqs", "redis", etc. ] allowed
	if c.SourceType == "" {
		return fmt.Errorf("source_type cannot be empty")
	}
	if c.Concurrency == 0 {
		return fmt.Errorf("concurrency cannot be 0")
	}
	return
}
