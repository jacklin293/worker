package worker

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Gracefull shutdown
type signalHandler struct {
	beforeClose     func()
	ctx             context.Context
	logger          *log.Logger
	shutdownCh      chan bool
	shutdownTimeout int64 // Seconds
	sigCh           chan os.Signal
	wg              sync.WaitGroup
}

func newSignalHandler() *signalHandler {
	ctx := context.Background()
	return &signalHandler{
		sigCh:      make(chan os.Signal, 1),
		shutdownCh: make(chan bool),
		ctx:        ctx,
	}
}

// Capture system signal
func (s *signalHandler) capture() {
	signal.Notify(s.sigCh, syscall.SIGINT, syscall.SIGTERM) // SIGINT=2, SIGTERM=15
	select {
	case <-s.shutdownCh:
		s.shutdown()
	case <-s.sigCh:
		s.shutdown()
	}
}

func (s *signalHandler) shutdown() {
	s.logger.Printf("Terminating ... (pid: %d)\n", syscall.Getpid())
	s.beforeClose()

	// Waiting for every job being done until it excess timeout if it has been set
	c := make(chan struct{})
	go func() {
		defer close(c)
		s.wg.Wait()
	}()
	var cancel context.CancelFunc
	if s.shutdownTimeout > 0 {
		s.ctx, cancel = context.WithTimeout(s.ctx, time.Duration(s.shutdownTimeout)*time.Second)
		defer cancel()
	}
	select {
	case <-c:
	case <-s.ctx.Done():
		s.logger.Printf("Timeout: > %d sec(s)\n", s.shutdownTimeout)
	}
	s.logger.Printf("Terminated (pid: %d)\n", syscall.Getpid())
}
