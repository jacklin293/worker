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
	sigs            chan os.Signal
	shutdownCh      chan bool
	shutdownTimeout int64 // Seconds
	beforeClose     func()

	wg     sync.WaitGroup
	logger *log.Logger
	ctx    context.Context
}

func newSignalHandler() *signalHandler {
	ctx := context.Background()
	return &signalHandler{
		sigs:       make(chan os.Signal, 1),
		shutdownCh: make(chan bool),
		ctx:        ctx,
	}
}

// Capture system signal
func (s *signalHandler) capture() {
	signal.Notify(s.sigs, syscall.SIGINT, syscall.SIGTERM) // SIGINT=2, SIGTERM=15
	select {
	case <-s.shutdownCh:
		s.shutdown()
	case <-s.sigs:
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
	if s.shutdownTimeout > 0 {
		s.ctx, _ = context.WithTimeout(s.ctx, time.Duration(s.shutdownTimeout)*time.Second)
	}
	select {
	case <-c:
	case <-s.ctx.Done():
		s.logger.Printf("Timeout: > %d sec(s)\n", s.shutdownTimeout)
	}
	s.logger.Printf("Terminated (pid: %d)\n", syscall.Getpid())
}
