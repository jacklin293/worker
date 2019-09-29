package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"worker/queue"
)

// ProjectName
type Handler struct {
	fetchers map[string][]*fetcher
	workers  map[string]*worker
	config   []*queue.Config

	doneChan chan *Job

	// When job is done, notify someone whom is interested in.
	// New() won't register it, leave it nil to disable as default
	notifyChan chan *Job

	// TODO
	// log *io.Writer
}

func New() *Handler { // FIXME func should be named as Project name
	var h Handler
	h.fetchers = make(map[string][]*fetcher)
	h.workers = make(map[string]*worker)
	h.doneChan = make(chan *Job)
	return &h
}

// Initialisation with config in json
func (h *Handler) InitWithJsonConfig(conf string) {
	if err := json.Unmarshal([]byte(conf), &h.config); err != nil {
		log.Fatalf("Failed to set config. Error: %v\n", err)
	}
	if len(h.config) == 0 {
		log.Fatal("No queues found")
	}

	for _, c := range h.config {
		if err := c.Validate(); err != nil {
			log.Fatal("Failed to set config. Error: ", err)
		}
	}

	for _, c := range h.config {
		if !c.Enabled {
			continue
		}

		// New queue
		q, err := c.GetQueueAttr().New()
		if err != nil {
			log.Fatal("Failed to new queue. Error: ", err)
		}

		// New worker
		w, ok := h.workers[c.Name]
		if !ok {
			w = newWorker(c.WorkerConcurrency)
			h.workers[c.Name] = w
		}
		w.config = c
		w.queue = q
		w.doneChan = h.doneChan
	}
}

func (h *Handler) Run() {
	if h.config == nil {
		log.Fatal("Please set config before running")
	}
	for _, c := range h.config {
		if !c.Enabled {
			continue
		}
		h.runWorkers(c)
		h.runFetchers(c)
	}
	go h.done()
}

// Process messages
func (h *Handler) runWorkers(c *queue.Config) {
	w := h.workers[c.Name]
	for i := int64(0); i < c.WorkerConcurrency; i++ {
		go func(w *worker, i int64) {
			for {
				j := <-w.receivedChan
				w.process(i, j)
			}
		}(w, i)
	}
}

// Receive messages
func (h *Handler) runFetchers(c *queue.Config) {
	w := h.workers[c.Name]
	for i := int64(0); i < c.QueueConcurrency; i++ {
		f := newFetcher()
		f.worker = w
		h.fetchers[c.Name] = append(h.fetchers[c.Name], f)
		go f.receive()
	}
}

func (h *Handler) done() {
	for {
		j := <-h.doneChan
		if h.notifyChan != nil {
			go func(h *Handler, j *Job) {
				h.notifyChan <- j
			}(h, j)
		}
		// TODO Graceful shutdown
	}
}

// New job type
func (h *Handler) RegisterJobType(name string, jobType string, p process) {
	if name == "" || jobType == "" {
		log.Fatal("Both queue name and job type can't be empty")
	}
	// Prevent panic from not being in the list of config
	if _, ok := h.workers[name]; !ok {
		return
	}
	h.workers[name].jobTypes[jobType] = p
}

func (h *Handler) GetFetcherNum() map[string]int {
	mm := make(map[string]int)
	for n, fetchers := range h.fetchers {
		mm[n] = len(fetchers)
	}
	return mm
}

func (h *Handler) GetWorkerStatus() map[string][]Job {
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

func (h *Handler) GetJobTypeList() map[string][]string {
	mm := make(map[string][]string)
	for n, w := range h.workers {
		for typ := range w.jobTypes {
			mm[n] = append(mm[n], typ)
		}
	}
	return mm
}

func (h *Handler) SetNotifyChan(ch chan *Job) {
	h.notifyChan = ch
}

// Queue name
func (h *Handler) GetQueueByName(name string) (queue.Queuer, error) {
	if _, ok := h.workers[name]; ok {
		return h.workers[name].queue, nil
	}
	return nil, fmt.Errorf("Failed to get queue. Error: queue name not matched")
}
