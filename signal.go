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
	sigs       chan os.Signal
	shutdownCh chan bool
	stopCall   func()

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
	case <-s.sigs:
		log.Println("Terminating ...")
		s.stopCall()
		s.wg.Wait()
		log.Println("Terminated")
		os.Exit(0)
	}
}
