package worker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"worker/queue"
)

// ProjectName
type Handler struct {
	fetchers map[string][]*fetcher
	workers  map[string]*worker
	config   *config

	doneChan chan *Job

	signalHandler  *signalHandler
	jobDoneCounter int64

	logger *log.Logger
}

func New() *Handler { // FIXME func should be named as Project name
	var h Handler
	h.fetchers = make(map[string][]*fetcher)
	h.workers = make(map[string]*worker)
	h.doneChan = make(chan *Job)
	h.logger = log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)
	h.signalHandler = newSignalHandler()
	h.signalHandler.beforeClose = h.beforeClose
	h.signalHandler.logger = h.logger
	return &h
}

// Initialisation with config in json
func (h *Handler) InitWithJsonConfig(conf string) {
	if err := json.Unmarshal([]byte(conf), &h.config); err != nil {
		h.logger.Fatalf("Failed to set config. Error: %v\n", err)
	}
	if len(h.config.Queues) == 0 {
		h.logger.Fatal("No queues found")
	}

	// Set shutdown timeout
	h.signalHandler.shutdownTimeout = h.config.ShutdownTimeout

	if !h.config.LogEnabled {
		h.logger.SetOutput(ioutil.Discard)
	}

	for _, c := range h.config.Queues {
		if err := c.Validate(); err != nil {
			h.logger.Fatal("Failed to set config. Error: ", err)
		}
	}

	// Initialisation
	for _, c := range h.config.Queues {
		if !c.Enabled {
			continue
		}

		h.newWorker(c)
		h.newFetcher(c)
	}
}

func (h *Handler) Run() {
	if h.config == nil {
		h.logger.Fatal("Please set config before running")
	}
	for _, c := range h.config.Queues {
		if !c.Enabled {
			continue
		}
		h.runWorkers(c)
		h.runFetchers(c)
	}
	go h.done()
	h.signalHandler.capture()
}

func (h *Handler) Shutdown() {
	close(h.signalHandler.shutdownCh)
}

func (h *Handler) beforeClose() {
	for _, fetchers := range h.fetchers {
		for _, f := range fetchers {
			close(f.stopQueueCh)
		}
	}
}

func (h *Handler) newWorker(c *queue.Config) {
	// New queue
	q, err := c.GetQueueAttr().New()
	if err != nil {
		h.logger.Fatal("Failed to new queue. Error: ", err)
	}

	w, ok := h.workers[c.Name]
	if !ok {
		w = newWorker(c.WorkerConcurrency)
		h.workers[c.Name] = w
	}
	w.config = c
	w.queue = q
	w.doneChan = h.doneChan
	w.logger = h.logger
}

// Process messages
func (h *Handler) runWorkers(c *queue.Config) {
	w, ok := h.workers[c.Name]
	if !ok {
		h.logger.Fatal("Please set config before running worker")
	}
	for i := int64(0); i < c.WorkerConcurrency; i++ {
		go w.dispatch(i)
	}
}

func (h *Handler) newFetcher(c *queue.Config) {
	for i := int64(0); i < c.QueueConcurrency; i++ {
		f := newFetcher()
		f.worker = h.workers[c.Name]
		f.signalHandler = h.signalHandler
		f.logger = h.logger
		f.config = c
		h.fetchers[c.Name] = append(h.fetchers[c.Name], f)
	}
}

// Receive messages
func (h *Handler) runFetchers(c *queue.Config) {
	fs, ok := h.fetchers[c.Name]
	if !ok {
		h.logger.Fatal("Please set config before running worker")
	}
	for i, f := range fs {
		go f.receive(int64(i))
	}
}

func (h *Handler) done() {
	for {
		<-h.doneChan
		h.signalHandler.wg.Done()
		h.jobDoneCounter++
	}
}

// New job type
func (h *Handler) RegisterJobType(name string, jobType string, p process) {
	if name == "" || jobType == "" {
		h.logger.Fatal("Both queue name and job type can't be empty")
	}
	// Prevent panic from not being in the list of config
	if _, ok := h.workers[name]; !ok {
		return
	}
	h.workers[name].jobTypes[jobType] = p
}

func (h *Handler) FetcherNum() map[string]int {
	mm := make(map[string]int)
	for qName, fetchers := range h.fetchers {
		mm[qName] = len(fetchers)
	}
	return mm
}

func (h *Handler) WorkerStatus() map[string][]Job {
	mm := make(map[string][]Job)
	for name, w := range h.workers {
		mm[name] = make([]Job, w.config.WorkerConcurrency)
		w.workerStatus.mutex.RLock()
		for i := int64(0); i < w.config.WorkerConcurrency; i++ {
			if job, ok := w.workerStatus.table[i]; ok {
				mm[name][i] = *job
			}
		}
		w.workerStatus.mutex.RUnlock()
	}
	return mm
}

func (h *Handler) JobTypeList() map[string][]string {
	mm := make(map[string][]string)
	for n, w := range h.workers {
		for typ := range w.jobTypes {
			mm[n] = append(mm[n], typ)
		}
	}
	return mm
}

func (h *Handler) JobDoneCounter() int64 {
	return h.jobDoneCounter
}

// Get Queue resource by name
func (h *Handler) Queue(name string) (queue.QueueContainer, error) {
	if _, ok := h.workers[name]; ok {
		return h.workers[name].queue, nil
	}
	return nil, fmt.Errorf("Failed to get queue. Error: queue name not matched")
}
