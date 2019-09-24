package worker

import (
	"log"
	"sync"
	"worker/source"
)

type worker struct {
	config       *source.Config
	source       source.Sourcer
	jobTypes     map[string]Runner
	receivedChan chan *Job
	doneChan     chan *Job
	status       map[int64]*Job
	mutex        sync.RWMutex

	// TODO
	// log *io.Writer
}

func newWorker(c *source.Config) worker {
	return worker{
		config:       c,
		receivedChan: make(chan *Job),
		jobTypes:     make(map[string]Runner),
		status:       make(map[int64]*Job, c.WorkerConcurrency),
	}
}

func (w *worker) run() {
	for i := int64(0); i < w.config.WorkerConcurrency; i++ {
		go func(w *worker, i int64) {
			for {
				j := <-w.receivedChan
				w.allocate(i, j)
			}
		}(w, i)
	}
}

func (w *worker) allocate(i int64, j *Job) {
	defer func() {
		if e := recover(); e != nil {
			log.Printf("panic: %v, message: %+v\n", e, j.Desc)
			w.doneChan <- j
		}
	}()

	j.doneChan = w.doneChan

	w.updateJobStatus(true, i, j)
	j.process(w.jobTypes[j.Desc.JobType])
	w.updateJobStatus(false, i, j)
}

func (w *worker) updateJobStatus(b bool, i int64, j *Job) {
	w.mutex.Lock()
	if b {
		w.status[i] = j
	} else {
		delete(w.status, i)
	}
	w.mutex.Unlock()
}
