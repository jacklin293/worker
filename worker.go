package worker

import (
	"log"
	"sync"
	"worker/source"
)

type worker struct {
	config       source.Config
	source       source.Sourcer
	jobTypes     map[string]Runner
	receivedChan chan *Job
	doneChan     chan *Job
	status       map[int]*Job
	mutex        sync.RWMutex

	// TODO
	// log *io.Writer
}

func newWorker(c source.Config) worker {
	return worker{
		config:       c,
		receivedChan: make(chan *Job),
		jobTypes:     make(map[string]Runner),
		status:       make(map[int]*Job, c.Concurrency),
	}
}

func (w *worker) run() {
	for i := 0; i < w.config.Concurrency; i++ {
		go func(w *worker, i int) {
			for {
				j := <-w.receivedChan
				w.allocate(i, j)
			}
		}(w, i)
	}
}

func (w *worker) allocate(i int, j *Job) {
	defer func() {
		if e := recover(); e != nil {
			log.Printf("panic: %v, message: %+v\n", e, j.Desc)
			w.doneChan <- j
		}
	}()

	j.doneChan = w.doneChan

	w.updateStatus(true, i, j)
	j.process(w.jobTypes[j.Desc.JobType])
	w.updateStatus(false, i, j)
}

func (w *worker) updateStatus(b bool, i int, j *Job) {
	w.mutex.Lock()
	if b {
		w.status[i] = j
	} else {
		delete(w.status, i)
	}
	w.mutex.Unlock()
}
