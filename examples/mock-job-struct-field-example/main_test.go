package main

import (
	"testing"
	"worker"
)

type MockMySQL struct{}

func (m *MockMySQL) GetConn() string {
	return "fack connection"
}

func TestCase1(t *testing.T) {
	tj := &YourJob{
		db: &MockMySQL{},
	}

	// Mock message
	err := tj.Do(&worker.Message{})
	if err != nil {
		t.Error("want nil, got ", err)
	}
}
