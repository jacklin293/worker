package worker

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// FIXME
var Queue chan string

type Manager struct {
	Workers map[string]*Worker
	Topics  []string

	// TODO
	SQS struct {
		VisibleChan chan *Job
	}
	DoneChan chan *Job

	// TODO
	Log *io.Writer
}

func init() {
	// FIXME
	Queue = make(chan string)
}

func (m *Manager) Init() {
	m.Workers = make(map[string]*Worker)
	m.DoneChan = make(chan *Job)

	// TODO Validate
	if len(m.Topics) == 0 {
		panic("Please pass 1 topic at least")
	}

	for _, t := range m.Topics {
		// New worker
		var w Worker
		// TODO concurrency
		w.Topic = t
		w.Number = 30 // TODO
		w.DoneChan = m.DoneChan
		w.init()
		m.Workers[t] = &w
		go m.Receive(t)
	}
	go m.Done()
}

// TODO SQS Receive should be in package
func (m *Manager) Receive(t string) {
	for {
		body := <-Queue
		var j Job
		if err := json.Unmarshal([]byte(body), &j.Desc); err != nil {
			fmt.Printf("Wrong job format: %s\n", body)
			// TODO Remove msg from queue
		}
		fmt.Println("Receive: " + body)

		j.Topic = t
		j.ReceivedAt = time.Now()
		m.Workers[t].ReceivedChan <- &j
	}
}

func (m *Manager) Done() {
	for {
		j := <-m.DoneChan
		j.DoneAt = time.Now()
		j.Duration = j.DoneAt.Sub(j.ReceivedAt)
		fmt.Printf("Job done, Duration: %.1fs, Topic: %s, Type: %s, ID: %s\n", j.Duration.Seconds(), j.Topic, j.Desc.Type, j.Desc.ID)
	}
}

// TODO validation
// Do(), topic, jobtype
func (m *Manager) Register(j *Job) {
	fmt.Printf("Register %s[%s]\n", j.Topic, j.Desc.Type)
	// FIXME
	// validate do()
	if j.Topic == "" || j.Desc.Type == "" {

	}
	m.Workers[j.Topic].JobTypes[j.Desc.Type] = j
}

// TODO
func (m *Manager) JobTypes() {

}
