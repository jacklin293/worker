package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"
	"worker/source"
)

// ProjectName
type handler struct {
	workers map[string]*worker
	config  []*source.Config
	jobPool *sync.Pool

	// TODO
	// sqs struct {
	//	VisibleChan chan *Job
	// }
	doneChan chan *Job

	// When job is done, notify someone whom is interested in.
	// New() won't register it, leave it nil to disable as default
	notifyChan chan *Job

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

// Initialisation with config in json
func (m *handler) SetConfigWithJSON(conf string) {
	if err := json.Unmarshal([]byte(conf), &m.config); err != nil {
		log.Fatalf("Failed to set config. Error: %v\n", err)
	}
	if len(m.config) == 0 {
		log.Fatal("No any sources found")
	}

	for _, c := range m.config {
		if err := c.Validate(); err != nil {
			log.Fatal("Failed to set config. Error: ", err)
		}
	}
}

func (m *handler) Run() {
	if m.config == nil {
		log.Fatal("Please set config before running")
	}
	for _, c := range m.config {
		if !c.Enabled {
			continue
		}

		// New worker
		w := newWorker(c)
		w.doneChan = m.doneChan
		w.source = c.GetSourceAttr().New()
		w.run()
		m.workers[c.Name] = &w

		// Receive messages
		for i := int64(0); i < c.SourceConcurrency; i++ {
			go m.receive(&w)
		}
	}
	go m.done()
}

func (m *handler) receive(w *worker) {
	for {
		messages, err := w.source.Receive()
		if err != nil {
			log.Println("Error: ", err)
			continue
		}
		if len(messages) == 0 {
			continue
		}
		for _, msg := range messages {
			var j Job
			if err = m.processMessage(w.config, &msg, &j); err != nil {
				log.Printf("Error: %s, message: %s\n", err, string(msg))
			}
		}
	}
}

func (m *handler) processMessage(c *source.Config, msg *[]byte, j *Job) (err error) {
	if err = json.Unmarshal(*msg, &j.Desc); err != nil {
		return
	}
	if err = j.validate(); err != nil {
		return
	}
	if _, ok := m.workers[c.Name].jobTypes[j.Desc.JobType]; !ok {
		log.Printf("Job type '%s'.'%s' not found\n", c.Name, j.Desc.JobType)
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
func (m *handler) RegisterJobType(jb Runner, name string, jobType string) {
	if name == "" || jobType == "" {
		log.Fatal("Both source name and job type cannot be empty")
	}
	if reflect.ValueOf(jb).Kind() == reflect.Ptr {
		log.Fatalf("Can not use pointer for registering a job '%s'\n", jobType)
	}
	// Prevent from panic due to name that is not in the config
	if _, ok := m.workers[name]; !ok {
		return
	}
	m.workers[name].jobTypes[jobType] = jb
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

// Source name
func (m *handler) GetSourceByName(name string) (source.Sourcer, error) {
	if _, ok := m.workers[name]; ok {
		return m.workers[name].source, nil
	}
	return nil, fmt.Errorf("Failed to get source. Error: source name not matched")
}
