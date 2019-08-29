package worker

import (
	"fmt"
	"io"
)

type Worker struct {
	// FIXME nameing  Topic to Queue
	Topic string

	JobTypes map[string]*Job

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
	w.JobTypes = make(map[string]*Job)
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

	// TODO Pointer helps?
	j.Do = w.JobTypes[j.Desc.Type].Do
	j.Fail = w.JobTypes[j.Desc.Type].Fail
	j.Succeed = w.JobTypes[j.Desc.Type].Succeed
	j.Done = w.JobTypes[j.Desc.Type].Done
	j.DoneChan = w.DoneChan

	w.Status[i] = j
	j.process()
	delete(w.Status, i)
}
