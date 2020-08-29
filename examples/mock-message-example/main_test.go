package main

import (
	"testing"
	"worker"
)

type MockMySQL struct{}

func (m *MockMySQL) GetConn() string {
	return "Fack Conn"
}

// Mock message
// inherit methods from another struct
type mockMsg struct {
	worker.Message
}

// override the payload method
func (m *mockMsg) Payload() interface{} {
	return "FOO"
}

func TestCase1(t *testing.T) {
	var mysql *MockMySQL

	tj := &TestJob{
		mysql: mysql,
	}

	// Mock message
	mock := &mockMsg{}
	err := tj.Do(mock)
	if err != nil {
		t.Error("want nil, got ", err)
	}
}
