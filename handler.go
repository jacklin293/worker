package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
	"worker/source"
)

// ProjectName
type handler struct {
	workers map[string]*worker
	config  []*source.Config

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
	return &m
}

// Initialisation with config in json
func (m *handler) InitWithJsonConfig(conf string) {
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

	for _, c := range m.config {
		if !c.Enabled {
			continue
		}

		// New source
		s, err := c.GetSourceAttr().New()
		if err != nil {
			log.Fatal("Failed to new source type. Error: ", err)
		}

		// New worker
		w, ok := m.workers[c.Name]
		if !ok {
			w = newWorker(c)
			m.workers[c.Name] = w
		}
		w.doneChan = m.doneChan
		w.source = s
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
		w := m.workers[c.Name]
		w.run()

		// Receive messages
		for i := int64(0); i < c.SourceConcurrency; i++ {
			go m.receive(w)
		}
	}
	go m.done()
}

func (m *handler) receive(w *worker) {
	for {
		message, err := w.source.Receive()
		if err != nil {
			log.Println("Error: ", err)
			continue
		}

		// Check the type of return from Receive()
		switch message.(type) {
		case [][]byte:
			if len(message.([][]byte)) == 0 {
				continue
			}
			for _, msg := range message.([][]byte) {
				var j Job
				if err = m.processMessage(w, msg, &j); err != nil {
					log.Printf("Error: %s, message: %s\n", err, string(msg))
				}
			}
		case []byte:
			if len(message.([]byte)) == 0 {
				continue
			}
			var j Job
			if err = m.processMessage(w, message.([]byte), &j); err != nil {
				log.Printf("Error: %s, message: %s\n", err, string(message.([]byte)))
			}
		default:
			log.Println("Error: unknown type of return from Receive()")
			continue
		}
	}
}

func (m *handler) processMessage(w *worker, msg []byte, j *Job) (err error) {
	if err = json.Unmarshal(msg, &j.Desc); err != nil {
		return
	}
	if err = j.validate(); err != nil {
		return
	}
	if _, ok := w.jobTypes[j.Desc.JobType]; !ok {
		log.Printf("Job type '%s'.'%s' not found\n", w.config.Name, j.Desc.JobType)
		return
	}
	j.receivedAt = time.Now()
	w.receivedChan <- j
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
func (m *handler) RegisterJobType(name string, jobType string, s sign) {
	if name == "" || jobType == "" {
		log.Fatal("Both source name and job type can't be empty")
	}
	// Prevent panic from not being in the list of config
	if _, ok := m.workers[name]; !ok {
		return
	}
	m.workers[name].jobTypes[jobType] = s
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
