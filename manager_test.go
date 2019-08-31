package worker

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewAndGetTopics(t *testing.T) {
	t.Parallel()
	cases := []struct {
		topics []Topic
		hasErr bool
	}{
		{[]Topic{Topic{}}, true},
		{[]Topic{Topic{"", 1, "foo"}}, true},
		{[]Topic{Topic{"test-topic-1", 0, "foo"}}, true},
		{[]Topic{Topic{"test-topic-1", 1, ""}}, true},
		{[]Topic{Topic{"test-topic-1", 1, "foo"}}, false},
		{[]Topic{Topic{"test-topic-1", 1, "foo"}, Topic{"test-topic-2", 1, "foo"}}, false}}
	for _, tc := range cases {
		m, err := New(tc.topics)
		if tc.hasErr {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, tc.topics, m.GetTopics())
		}
	}
}

func TestReceive(t *testing.T) {
	t.Skip()
}

func TestDone(t *testing.T) {
	t.Skip()
}

func TestRegister(t *testing.T) {
	t.Parallel()
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
		{JobBehaviour(TestJob{}), "test-topic-2", "test-job_type-1", false},
		{JobBehaviour(TestJob{}), "test-topic-2", "test-job_type-2", false},
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

func TestGetJobTypes(t *testing.T) {
	t.Parallel()
	m, _ := New([]Topic{Topic{"test-topic-1", 1, "foo"}})
	j := JobBehaviour(TestJob{})
	m.Register(j, "test-topic-1", "foo-1")
	m.Register(j, "test-topic-1", "foo-2")
	m.Register(j, "test-topic-1", "foo-3")
	m.Register(j, "test-topic-2", "foo-1")
	m.Register(j, "test-topic-2", "foo-2")
	m.Register(j, "test-topic-2", "foo-2") // repeat
	// FIXME Fail sometimes
	assert.True(t, reflect.DeepEqual(m.GetJobTypes(), map[string][]string{
		"test-topic-1": []string{"foo-1", "foo-2", "foo-3"},
		"test-topic-2": []string{"foo-1", "foo-2"},
	}))
}
