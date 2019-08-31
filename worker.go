package worker

import (
	"errors"
	"fmt"
	"io"
)

type worker struct {
	// FIXME nameing  Topic to Queue
	topic        string
	jobTypes     map[string]JobBehaviour
	number       int
	receivedChan chan *Job
	doneChan     chan *Job
	status       map[int]*Job

	// TODO
	log *io.Writer
}

type Topic struct {
	Name         string
	WorkerNumber int
	Endpoint     string
}

func newWorker(t string, n int) worker {
	return worker{
		topic:        t,
		number:       n,
		receivedChan: make(chan *Job),
		jobTypes:     make(map[string]JobBehaviour),
		status:       make(map[int]*Job, n),
	}
}

func (w *worker) run() {
	for i := 0; i < w.number; i++ {
		go w.allocate(i)
	}
}

func (w *worker) allocate(i int) {
	j := <-w.receivedChan
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("Worker recover) error may be caused by undefined job type. err: %v, job description: %+v\n", e, j.Desc)
			// FIXME
			w.doneChan <- j
		}
	}()

	j.doneChan = w.doneChan

	w.status[i] = j
	j.process(w.jobTypes[j.Desc.JobType])
	delete(w.status, i)
}

func (t *Topic) validate() (err error) {
	if t.Name == "" {
		return errors.New("Topic name cannot be empty")
	}
	if t.WorkerNumber == 0 {
		return fmt.Errorf("Topic '%s' worker number cannot be 0", t.Name)
	}
	if t.Endpoint == "" {
		return fmt.Errorf("Topic '%s' endpoint cannot be empty", t.Name)
	}
	return
}
