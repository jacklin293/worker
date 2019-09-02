package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"time"
)

// FIXME
var Queue chan string

// ProjectName
type manager struct {
	workers map[string]*worker
	topics  []Topic

	// TODO
	sqs struct {
		VisibleChan chan *Job
	}
	doneChan chan *Job

	// TODO
	log *io.Writer
}

func init() {
	// FIXME
	Queue = make(chan string)
}

func New(topics []Topic) (*manager, error) {
	var m manager
	m.topics = topics
	m.workers = make(map[string]*worker)
	m.doneChan = make(chan *Job)

	// TODO Validate
	if len(topics) == 0 {
		return nil, errors.New("Cannot be initialised with no topic")
	}
	for _, t := range topics {
		if err := t.validate(); err != nil {
			return nil, err
		}
	}
	return &m, nil
}

func (m *manager) Run() {
	for _, t := range m.topics {
		// New worker
		w := newWorker(t.Name, t.WorkerNumber)
		w.doneChan = m.doneChan
		w.run()
		m.workers[t.Name] = &w

		// Receive messages
		go m.receive(t.Name)
	}
	go m.done()
}

// TODO SQS Receive should be in package
func (m *manager) receive(t string) {
	var err error
	for {
		body := <-Queue // TODO Received
		var j Job
		if err = json.Unmarshal([]byte(body), &j.Desc); err != nil {
			log.Printf("Wrong job format: %s\n", body)
			// TODO Remove msg from queue
			continue
		}
		if err = j.validate(); err != nil {
			log.Printf("Wrong job format: %s\n", body)
			// TODO Remove msg from queue
			continue
		}
		// FIXME
		fmt.Println("Receive: " + body)

		j.Topic = t
		j.receivedAt = time.Now()
		m.workers[t].receivedChan <- &j
	}
}

func (m *manager) done() {
	for {
		j := <-m.doneChan
		j.doneAt = time.Now()
		j.duration = j.doneAt.Sub(j.receivedAt)
		fmt.Printf("Job done, Duration: %.1fs, Topic: %s, Type: %s, ID: %s\n", j.duration.Seconds(), j.Topic, j.Desc.JobType, j.Desc.JobID)
		if j.Done != nil {
			j.Done()
		}
	}
}

// For mocking purpose
func (m *manager) setDoneChan(ch chan *Job) {
	m.doneChan = ch
}

// Register job type
func (m *manager) Register(j JobBehaviour, topicName string, jobType string) (err error) {
	if topicName == "" || jobType == "" {
		return errors.New("Both topic and job type cannot be empty")
	}
	if reflect.ValueOf(j).Kind() == reflect.Ptr {
		return fmt.Errorf("Do not use pointer for registering a job '%s'\n", jobType)
	}
	// Prevent panic from topics which are not in the list
	if _, ok := m.workers[topicName]; !ok {
		w := newWorker(topicName, 0)
		m.workers[topicName] = &w
	}
	m.workers[topicName].jobTypes[jobType] = j
	return
}

func (m *manager) GetTopics() []Topic {
	return m.topics
}

func (m *manager) GetJobTypes() (mm map[string][]string) {
	mm = make(map[string][]string)
	for t, w := range m.workers {
		for typ, _ := range w.jobTypes {
			mm[t] = append(mm[t], typ)
		}
	}
	return
}
