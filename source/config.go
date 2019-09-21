package source

import (
	"errors"
	"fmt"
)

type Config struct {
	Name        string `json:"name"`
	Queue       string `json:"queue"`
	Endpoint    string `json:"endpoint"`
	topic       string `json:"topic"`
	Concurrency int    `json:"concurrency"`
	Enabled     bool   `json:"enabled"`
}

func (c *Config) New() Sourcer {
	switch c.Queue {
	case "go-channel":
		return &GoChannel{receivedCh: make(chan []byte)}
	}
	return &GoChannel{}
}

func (c *Config) Validate() (err error) {
	// TODO only contain a-z, A-Z, -, _   unique name
	if c.Name == "" {
		return errors.New("source name cannot be empty")
	}
	// TODO only contain a-z, A-Z, -, _   unique name
	if c.Queue == "" {
		return fmt.Errorf("source '%s' queue cannot be empty", c.Name)
	}
	if c.Concurrency == 0 {
		return fmt.Errorf("source '%s' concurrency cannot be 0", c.Name)
	}
	return
}
