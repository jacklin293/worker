package main

import (
	"testing"
	"worker"
)

// Mock message
// inherit methods from another struct
type mockMsg struct{ worker.Message }

// override the payload method
func (m *mockMsg) Payload() interface{} { return "FOO" }

func TestCase1(t *testing.T) {
	tj := &TestJob{}

	// Mock message
	mock := &mockMsg{}
	tj.Run(mock)
}
