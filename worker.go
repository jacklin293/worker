package worker

import (
	"fmt"
	"io"
)

type workerConfig struct {
	Env       EnvConfig
	Container ContainerConfig
}

type worker struct {
	config       *workerConfig
	jobTypes     map[string]JobBehaviour
	concurrency  int
	receivedChan chan *Job
	doneChan     chan *Job
	status       map[int]*Job

	// TODO
	log *io.Writer
}

func newWorker(c *workerConfig) worker {
	return worker{
		config:       c,
		receivedChan: make(chan *Job),
		jobTypes:     make(map[string]JobBehaviour),
		status:       make(map[int]*Job, c.Container.Concurrency),
	}
}

func (w *worker) run() {
	for i := 0; i < w.config.Container.Concurrency; i++ {
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
	j.Config = w.config

	w.status[i] = j
	j.process(w.jobTypes[j.Desc.JobType])
	delete(w.status, i)
}
