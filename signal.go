package worker

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// Gracefull shutdown
type signalHandler struct {
	sigs        chan os.Signal
	shutdownCh  chan bool
	beforeClose func()

	wg sync.WaitGroup
	// TODO io.Writer
}

func newSignalHandler() *signalHandler {
	return &signalHandler{
		sigs:       make(chan os.Signal, 1),
		shutdownCh: make(chan bool),
	}
}

// Capture system signal
func (s *signalHandler) capture() {
	signal.Notify(s.sigs, syscall.SIGINT, syscall.SIGTERM) // SIGINT=2, SIGTERM=15
	select {
	case <-s.shutdownCh:
		s.close()
	case <-s.sigs:
		s.close()
	}
}

func (s *signalHandler) close() {
	log.Printf("Terminating ... (pid: %d)\n", syscall.Getpid())
	s.beforeClose()
	s.wg.Wait()
	log.Printf("Terminated (pid: %d)\n", syscall.Getpid())
}
