package worker

import (
	"log"
	"sync"
	"worker/queue"
)

type worker struct {
	config          *queue.Config
	doneMessageCh   chan *message
	jobTypes        map[string]jobTypeFunc
	logger          *log.Logger
	undoneMessageCh chan *message
	queue           queue.QueueContainer
	workerStatus    *workerStatus
}

type workerStatus struct {
	mutex sync.RWMutex
	list  map[int64]*message
}

func newWorker(concurrency int64) *worker {
	return &worker{
		undoneMessageCh: make(chan *message),
		jobTypes:        make(map[string]jobTypeFunc),
		workerStatus:    &workerStatus{list: make(map[int64]*message, concurrency)},
	}
}

func (w *worker) receive(i int64) {
	for {
		msg := <-w.undoneMessageCh
		w.process(i, msg)
	}
}

func (w *worker) process(i int64, msg *message) {
	defer func() {
		if e := recover(); e != nil {
			w.logger.Printf("panic: %v, payload: %+v\n", e, msg.descriptor)
			w.doneMessageCh <- msg
		}
	}()

	msg.doneMessageCh = w.doneMessageCh

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
