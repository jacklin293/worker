package worker

import "fmt"

type Worker struct {
	Topic string

	JobNames map[string]*Job

	Number int

	// TODO
	Validate func(string) error

	ReceivedChan chan *Job
	DoneChan     chan *Job
	Status       map[int]*Job

	// TODO Log *logrus.Entry
	// TODO Config
}

func (w *Worker) init() {
	w.ReceivedChan = make(chan *Job)
	w.JobNames = make(map[string]*Job)
	w.Status = make(map[int]*Job, w.Number)

	for i := 0; i < w.Number; i++ {
		go w.run(i)
	}
}

func (w *Worker) run(i int) {
	for {
		w.allocate(i, <-w.ReceivedChan)
	}
}

func (w *Worker) allocate(i int, j *Job) {
	defer func(j *Job) {
		if e := recover(); e != nil {
			fmt.Printf("Worker recover) error may be caused by undefined job name. err: %v, job description: %+v\n", e, j.Desc)
			// FIXME
			w.DoneChan <- j
		}
	}(j)

	j.Do = w.JobNames[j.Desc.Name].Do
	j.Fail = w.JobNames[j.Desc.Name].Fail
	j.Succeed = w.JobNames[j.Desc.Name].Succeed
	j.Done = w.JobNames[j.Desc.Name].Done

	w.Status[i] = j
	j.process()
	delete(w.Status, i)
}
