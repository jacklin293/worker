package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"
)

type sourceConfig struct {
	Name        string `json:"name"`
	Provider    string `json:"provider"`
	Endpoint    string `json:"endpoint"`
	topic       string `json:"topic"`
	Concurrency int    `json:"concurrency"`
	Enabled     bool   `json:"enabled"`
}

// ProjectName
type handler struct {
	workers map[string]*worker
	config  []sourceConfig
	jobPool *sync.Pool

	// TODO
	// sqs struct {
	//	VisibleChan chan *Job
	// }
	doneChan chan *Job
	// When job is done, notify someone who is interested in.
	// New() won't initialise it
	notifyChan  chan *Job
	receiveChan chan []byte

	// TODO
	// log *io.Writer
}

func New() *handler { // FIXME func should be named as Project name
	var m handler
	m.workers = make(map[string]*worker)
	m.doneChan = make(chan *Job)
	m.jobPool = &sync.Pool{
		New: func() interface{} {
			return &Job{}
		},
	}
	return &m
}

func (m *handler) SetConfig() {

}

// Initialisation with config in json
func (m *handler) SetConfigWithJSON(conf string) {
	if err := json.Unmarshal([]byte(conf), &m.config); err != nil {
		log.Fatalf("Failed to parse config, err: %v\n", err)
	}
	if len(m.config) == 0 {
		log.Fatal("No source configs")
	}

	workerEnabled := false
	for _, c := range m.config {
		if err := c.validate(); err != nil {
			log.Fatal("Config err: ", err)
		}
		if c.Enabled {
			workerEnabled = true
		}
	}
	if !workerEnabled {
		log.Fatal("None of sources are enabled")
	}
}

func (m *handler) Run() {
	if m.config == nil {
		log.Fatal("Please set config before running")
	}
	for _, c := range m.config {
		// TODO
		// check validate, if available source is zero, do log.Fatal

		// New worker
		w := newWorker(c)
		w.doneChan = m.doneChan
		w.run()
		m.workers[c.Name] = &w

		// Receive messages
		go m.receive(c)
	}
	go m.done()
}

// TODO SQS Receive should be in package
func (m *handler) receive(c sourceConfig) { // TODO pass config
	var err error
	for {
		body := <-m.receiveChan // TODO Received
		var j Job
		if err = m.processMessage(c, &body, &j); err != nil {
			log.Printf("Wrong job format: %s\n", body)
		}
	}
}

func (m *handler) processMessage(c sourceConfig, msg *[]byte, j *Job) (err error) {
	if err = json.Unmarshal(*msg, &j.Desc); err != nil {
		return
	}
	if err = j.validate(); err != nil {
		return
	}
	if _, ok := m.workers[c.Name].jobTypes[j.Desc.JobType]; !ok {
		log.Printf("Job type '%s'.'%s' is not initialised\n", c.Name, j.Desc.JobType)
		return
	}
	j.receivedAt = time.Now()
	m.workers[c.Name].receivedChan <- j
	return
}

func (m *handler) done() {
	for {
		j := <-m.doneChan
		if m.notifyChan != nil {
			go func(m *handler, j *Job) {
				m.notifyChan <- j
			}(m, j)
		}
		// TODO Graceful shutdown
	}
}

// New job type
func (m *handler) InitJobType(jb JobBehaviour, sourceName string, jobType string) {
	if sourceName == "" || jobType == "" {
		log.Fatal("Both source name and job type cannot be empty")
	}
	if reflect.ValueOf(jb).Kind() == reflect.Ptr {
		log.Fatalf("Can not use pointer for initialising a job '%s'\n", jobType)
	}
	// Prevent from panic due to the fact that source name s not in the list
	if _, ok := m.workers[sourceName]; !ok {
		w := newWorker(m.workers[sourceName].config)
		m.workers[sourceName] = &w
	}
	m.workers[sourceName].jobTypes[jobType] = jb
}

func (m *handler) GetJobTypes() (mm map[string][]string) {
	mm = make(map[string][]string)
	for t, w := range m.workers {
		for typ := range w.jobTypes {
			mm[t] = append(mm[t], typ)
		}
	}
	return
}

func (m *handler) SetNotifyChan(ch chan *Job) {
	m.notifyChan = ch
}

// FIXME for benchmark temporarily
func (m *handler) SetReceiveChan(ch chan []byte) {
	// FIXME
	m.receiveChan = ch
}

func (c sourceConfig) validate() (err error) {
	// TODO only contain a-z, A-Z, -, _   unique name
	if c.Name == "" {
		return errors.New("source name cannot be empty")
	}
	if c.Provider == "" {
		return fmt.Errorf("source '%s' provider cannot be empty", c.Name)
	}
	// FIXME Depends on which source used to decide whether to validate endpoint and source
	// if c.Endpoint == "" {
	//	return fmt.Errorf("source '%s' endpoint cannot be empty", c.Name)
	// }
	// if c.Source == "" {
	//	return fmt.Errorf("source '%s' source cannot be empty", c.Name)
	// }
	if c.Concurrency == 0 {
		return fmt.Errorf("source '%s' concurrency cannot be 0", c.Name)
	}
	return
}
