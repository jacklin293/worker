package main

import (
	"testing"
	"worker"
)

// Mock message
type mockMsg struct {
	worker.Message
}

func (m *mockMsg) Payload() interface{} {
	return "FOO"
}

func TestCase1(t *testing.T) {
	tj := &YourJob{}

	// Mock message
	mock := &mockMsg{}
	err := tj.Do(mock)
	if err != nil {
		t.Error("want nil, got ", err)
	}
}
