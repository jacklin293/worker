package worker

import (
	"encoding/json"
	"fmt"
	"time"
)

// HACK
var Common chan string

type Manager struct {
	Workers map[string]*Worker
	Topics  []string

	// TODO
	SQS struct {
		VisibleChan chan *Job
	}
	DoneChan chan *Job
}

func init() {
	Common = make(chan string)
}

func (m *Manager) Init() {
	m.Workers = make(map[string]*Worker)
	m.DoneChan = make(chan *Job)

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

func (m *Manager) Receive(t string) {
	for {
		body := <-Common
		fmt.Println("Receive: " + body)
		var j Job
		if err := json.Unmarshal([]byte(body), &j.Desc); err != nil {
			fmt.Printf("Wrong job format: %s", body)
		}
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
		fmt.Printf("Job done, Duration: %.1fs, Topic: %s, Name: %s, ID: %s\n", j.Duration.Seconds(), j.Topic, j.Desc.Name, j.Desc.ID)
	}
}

func (m *Manager) Register(j *Job) {
	fmt.Printf("Register %s.%s\n", j.Topic, j.Desc.Name)
	m.Workers[j.Topic].JobNames[j.Desc.Name] = j
}
