package main

import (
	"testing"
	"worker"
)

// FIXME, it fails to override it
type MockYourJob struct {
	YourJob
}

func (m *MockYourJob) Custom() string {
	return "FOO"
}

func Do(j worker.Job) error {
	return j.Do(&worker.Message{})
}

func TestCase1(t *testing.T) {
	myj := &MockYourJob{}

	// Mock message
	// err := Do(myj)
	err := myj.Do(&worker.Message{})
	if err != nil {
		t.Error("want nil, got ", err)
	}
}
