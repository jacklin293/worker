package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"time"
)

// FIXME
var Queue chan string

type managerConfig struct {
	Env        EnvConfig         `json:"env"`
	Containers []ContainerConfig `json:"containers"`
}

type EnvConfig struct {
	Env string `json:"env"`
}

type ContainerConfig struct {
	Name        string `json:"name"`
	Provider    string `json:"provider"`
	Endpoint    string `json:"endpoint"`
	Source      string `json:"source"`
	Concurrency int    `json:"concurrency"`
	Enabled     bool   `json:"enabled"`
}

// ProjectName
type manager struct {
	workers map[string]*worker
	config  *managerConfig

	// TODO
	// sqs struct {
	//	VisibleChan chan *Job
	// }
	doneChan chan *Job

	// TODO
	// log *io.Writer
}

func init() {
	// FIXME
	Queue = make(chan string)
}

func New() *manager { // FIXME func should be named as Project name
	var m manager
	m.workers = make(map[string]*worker)
	m.doneChan = make(chan *Job)
	return &m
}

// Initialisation with config file
func (m *manager) SetConfigWithFile(path string) {
	// TODO Read config file into config struct
	// Config validation
	// if len(configs) == 0 {
	//	return nil, errors.New("Can not load config")
	// }
	// for _, c := range configs {
	//	if err := c.validate(); err != nil {
	//		return nil, err
	//	}
	// }
	m.config = &managerConfig{}
}

// Initialisation with config in json
func (m *manager) SetConfigWithJSON(conf string) {
	if err := json.Unmarshal([]byte(conf), &m.config); err != nil {
		log.Fatalf("Failed to parse config in JSON, err: %v\n", err)
	}
	if len(m.config.Containers) == 0 {
		log.Fatal("No container configs")
	}

	workerEnabled := false
	for _, c := range m.config.Containers {
		if err := c.validate(); err != nil {
			log.Fatal("Config err: ", err)
		}
		if c.Enabled {
			workerEnabled = true
		}
	}
	if !workerEnabled {
		log.Fatal("None of containers are enabled")
	}
}

func (m *manager) Run() {
	if m.config == nil {
		log.Fatal("Please set config before running")
	}
	for _, c := range m.config.Containers {
		// TODO
		// check validate, if available container is zero, do log.Fatal

		// New worker
		w := newWorker(&workerConfig{
			Env:       m.config.Env,
			Container: c,
		})
		w.doneChan = m.doneChan
		w.run()
		m.workers[c.Name] = &w

		// Receive messages
		go m.receive(&c)
	}
	go m.done()
}

// TODO SQS Receive should be in package
func (m *manager) receive(c *ContainerConfig) { // TODO pass config
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
		if _, ok := m.workers[c.Name].jobTypes[j.Desc.JobType]; !ok {
			log.Printf("Job type '%s' is not initialised for '%s'\n", j.Desc.JobType, c.Name)
			// TODO Remove msg from queue
			continue
		}

		// FIXME
		log.Println("Receive: " + body)

		j.receivedAt = time.Now()
		m.workers[c.Name].receivedChan <- &j
	}
}

func (m *manager) done() {
	for {
		j := <-m.doneChan
		log.Printf("Job done, ElapsedTime: %.1fs, ContainerName: %s, Type: %s, ID: %s\n", j.elapsedTime.Seconds(), j.Config.Container.Name, j.Desc.JobType, j.Desc.JobID)

		// TODO Graceful shutdown
	}
}

// New job type
func (m *manager) InitJobType(j JobBehaviour, containerName string, jobType string) {
	if containerName == "" || jobType == "" {
		log.Fatal("Both container name and job type cannot be empty")
	}
	if reflect.ValueOf(j).Kind() == reflect.Ptr {
		log.Fatalf("Do not use pointer for initialising a job '%s'\n", jobType)
	}
	// Prevent from panic due to the fact that container name s not in the list
	if _, ok := m.workers[containerName]; !ok {
		w := newWorker(&workerConfig{})
		m.workers[containerName] = &w
	}
	m.workers[containerName].jobTypes[jobType] = j
}

func (m *manager) GetJobTypes() (mm map[string][]string) {
	mm = make(map[string][]string)
	for t, w := range m.workers {
		for typ := range w.jobTypes {
			mm[t] = append(mm[t], typ)
		}
	}
	return
}

func (c *ContainerConfig) validate() (err error) {
	// TODO only contain a-z, A-Z, -, _   unique name
	if c.Name == "" {
		return errors.New("container name cannot be empty")
	}
	if c.Provider == "" {
		return fmt.Errorf("container '%s' provider cannot be empty", c.Name)
	}
	// FIXME Depends on which container used to decide whether to validate endpoint and source
	// if c.Endpoint == "" {
	//	return fmt.Errorf("container '%s' endpoint cannot be empty", c.Name)
	// }
	// if c.Source == "" {
	//	return fmt.Errorf("container '%s' source cannot be empty", c.Name)
	// }
	if c.Concurrency == 0 {
		return fmt.Errorf("container '%s' concurrency cannot be 0", c.Name)
	}
	return
}
