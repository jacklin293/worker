package worker

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	cases := []struct {
		topic  Topic
		hasErr bool
	}{
		{Topic{}, true},
		{Topic{"", 1, "foo"}, true},
		{Topic{"test-topic-1", 0, "foo"}, true},
		{Topic{"test-topic-1", 1, ""}, true},
		{Topic{"test-topic-1", 1, "foo"}, false},
	}
	for _, tc := range cases {
		_, err := New([]Topic{tc.topic})
		if tc.hasErr {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
		}
	}
}

func TestReceive(t *testing.T) {

}

func TestDone(t *testing.T) {

}

func TestRegister(t *testing.T) {
	m, _ := New([]Topic{Topic{"test-topic-1", 1, "foo"}})

	cases := []struct {
		j         JobBehaviour
		topicName string
		jobType   string
		hasErr    bool
	}{
		{JobBehaviour(TestJob{}), "", "test-job_type-1", true},
		{JobBehaviour(TestJob{}), "test-topic-1", "", true},
		{JobBehaviour(&TestJob{}), "test-topic-1", "test-job_type-1", true},
		{JobBehaviour(TestJob{}), "test-topic-1", "test-job_type-1", false},
	}
	for _, tc := range cases {
		err := m.Register(tc.j, tc.topicName, tc.jobType)
		if tc.hasErr {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
		}
	}
}

func TestGetTopics(t *testing.T) {
}

func TestGetJobTypes(t *testing.T) {
	m, _ := New([]Topic{Topic{"test-topic-1", 1, "foo"}})
	j := JobBehaviour(TestJob{})
	m.Register(j, "test-topic-1", "foo-1")
	m.Register(j, "test-topic-1", "foo-2")
	m.Register(j, "test-topic-1", "foo-3")
	m.Register(j, "test-topic-2", "foo-1")
	m.Register(j, "test-topic-2", "foo-2")
	m.Register(j, "test-topic-2", "foo-2") // repeat
	assert.True(t, reflect.DeepEqual(m.GetJobTypes(), map[string][]string{
		"test-topic-1": []string{"foo-1", "foo-2", "foo-3"},
		"test-topic-2": []string{"foo-1", "foo-2"},
	}))
}
