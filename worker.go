package worker

import (
	"log"
	"sync"
	"worker/queue"
)

type worker struct {
	config          *queue.Config
	logger          *log.Logger
	jobTypes        map[string]func() Job
	queue           queue.QueueContainer
	workerStatus    *workerStatus
	doneMessageCh   chan *Message
	undoneMessageCh chan *Message
}

type workerStatus struct {
	mutex sync.RWMutex
	list  map[int64]*Message
}

func newWorker(concurrency int64) *worker {
	return &worker{
		undoneMessageCh: make(chan *Message),
		jobTypes:        make(map[string]func() Job),
		workerStatus:    &workerStatus{list: make(map[int64]*Message, concurrency)},
	}
}

func (w *worker) receive(i int64) {
	for {
		msg := <-w.undoneMessageCh
		w.process(i, msg)
	}
}

func (w *worker) process(i int64, msg *Message) {
	defer func() {
		if e := recover(); e != nil {
			w.logger.Printf("panic: %v, payload: %+v\n", e, msg.descriptor)
			w.doneMessageCh <- msg
		}
	}()

	// New job type, then do the job
	w.flagWorkerStatus(true, i, msg)
	j := w.jobTypes[msg.descriptor.Type]()
	err := j.Do(msg)
	msg.done(j, err)
	w.doneMessageCh <- msg
	w.flagWorkerStatus(false, i, msg)
}

func (w *worker) flagWorkerStatus(b bool, i int64, msg *Message) {
	w.workerStatus.mutex.Lock()
	defer w.workerStatus.mutex.Unlock()
	if b {
		w.workerStatus.list[i] = msg
		return
	}
	delete(w.workerStatus.list, i)
}
