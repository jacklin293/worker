// +build unit

package worker

import "testing"

func TestNewWorker(t *testing.T) {

}

func TestTopicValidation(t *testing.T) {
	t.Skip()
	// cases := []struct {
	//	source Source
	//	hasErr bool
	// }{
	//	{JobBehaviour(TestJob{}), "", "test-job_type-1", true},
	//	{JobBehaviour(TestJob{}), "test-topic-1", "", true},
	//	{JobBehaviour(&TestJob{}), "test-topic-1", "test-job_type-1", true},
	//	{JobBehaviour(TestJob{}), "test-topic-1", "test-job_type-1", false},
	//	{JobBehaviour(TestJob{}), "test-topic-2", "test-job_type-1", false},
	//	{JobBehaviour(TestJob{}), "test-topic-2", "test-job_type-2", false},
	// }
}
