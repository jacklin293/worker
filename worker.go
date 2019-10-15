package worker

import (
	"log"
	"sync"
	"worker/queue"
)

type worker struct {
	config       *queue.Config
	doneChan     chan *message
	jobTypes     map[string]jobTypeFunc
	logger       *log.Logger
	receivedChan chan *message
	queue        queue.QueueContainer
	workerStatus *workerStatus
}

type workerStatus struct {
	mutex sync.RWMutex
	list  map[int64]*message
}

func newWorker(concurrency int64) *worker {
	return &worker{
		receivedChan: make(chan *message),
		jobTypes:     make(map[string]jobTypeFunc),
		workerStatus: &workerStatus{list: make(map[int64]*message, concurrency)},
	}
}

func (w *worker) dispatch(i int64) {
	for {
		msg := <-w.receivedChan
		w.process(i, msg)
	}
}

func (w *worker) process(i int64, msg *message) {
	defer func() {
		if e := recover(); e != nil {
			w.logger.Printf("panic: %v, payload: %+v\n", e, msg.descriptor)
			w.doneChan <- msg
		}
	}()

	msg.doneChan = w.doneChan

	w.flagWorkerStatus(true, i, msg)
	msg.process(w.jobTypes[msg.descriptor.Type])
	w.flagWorkerStatus(false, i, msg)
}

func (w *worker) flagWorkerStatus(b bool, i int64, msg *message) {
	w.workerStatus.mutex.Lock()
	defer w.workerStatus.mutex.Unlock()
	if b {
		w.workerStatus.list[i] = msg
		return
	}
	delete(w.workerStatus.list, i)
}
