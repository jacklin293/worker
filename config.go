package worker

import "worker/queue"

type config struct {
	LogEnabled      bool            `json:"log_enabled"`
	ShutdownTimeout int64           `json:"shutdown_timeout"` // Seconds
	Queues          []*queue.Config `json:"queues"`
}
