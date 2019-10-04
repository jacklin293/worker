package worker

import (
	"log"
	"sync"
	"worker/queue"
)

type worker struct {
	config       *queue.Config
	queue        queue.Queuer
	jobTypes     map[string]process
	receivedChan chan *Job
	doneChan     chan *Job
	workerStatus *workerStatus
	logger       *log.Logger
}

type workerStatus struct {
	table map[int64]*Job
	mutex sync.RWMutex
}

func newWorker(concurrency int64) *worker {
	return &worker{
		receivedChan: make(chan *Job),
		jobTypes:     make(map[string]process),
		workerStatus: &workerStatus{table: make(map[int64]*Job, concurrency)},
	}
}

func (w *worker) dispatch(i int64) {
	for {
		j := <-w.receivedChan
		w.process(i, j)
	}
}

func (w *worker) process(i int64, j *Job) {
	defer func() {
		if e := recover(); e != nil {
			w.logger.Printf("panic: %v, message: %+v\n", e, j.Desc)
			w.doneChan <- j
		}
	}()

	j.doneChan = w.doneChan

	w.flagWorkerStatus(true, i, j)
	j.process(w.jobTypes[j.Desc.JobType])
	w.flagWorkerStatus(false, i, j)
}

func (w *worker) flagWorkerStatus(b bool, i int64, j *Job) {
	w.workerStatus.mutex.Lock()
	defer w.workerStatus.mutex.Unlock()
	if b {
		w.workerStatus.table[i] = j
		return
	}
	delete(w.workerStatus.table, i)
}
