package worker

import (
	"fmt"
	"io"
)

type Worker struct {
	// FIXME nameing  Topic to Queue
	Topic string

	JobTypes map[string]JobBehaviour

	Number int

	// TODO
	Validate func(string) error

	ReceivedChan chan *Job
	DoneChan     chan *Job
	Status       map[int]*Job

	// TODO
	Log *io.Writer
}

func (w *Worker) init() {
	w.ReceivedChan = make(chan *Job)
	w.JobTypes = make(map[string]JobBehaviour)
	w.Status = make(map[int]*Job, w.Number)

	for i := 0; i < w.Number; i++ {
		go w.allocate(i)
	}
}

func (w *Worker) allocate(i int) {
	j := <-w.ReceivedChan
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("Worker recover) error may be caused by undefined job type. err: %v, job description: %+v\n", e, j.Desc)
			// FIXME
			w.DoneChan <- j
		}
	}()

	j.DoneChan = w.DoneChan

	w.Status[i] = j
	j.process(w.JobTypes[j.Desc.Type])
	delete(w.Status, i)
}
