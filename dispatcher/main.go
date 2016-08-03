package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	InitLog()
	log.Debugf("Conf: %+v", Conf)
	dispatcher := NewDispatcher(&Conf)
	dispatcher.Start()
	dispatcher.HandleSignal()
}

// HandleSignal fetch signal from chan then do exit or reload.
func (d *Dispatcher) HandleSignal() {
	// Block until a signal is received.
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT, syscall.SIGSTOP)
	for {
		s := <-c
		log.Infof("get a signal %s", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGINT:
			d.Stop()
			time.Sleep(1 * time.Second)
			return
		case syscall.SIGHUP:
			// TODO reload
			//return
		default:
			return
		}
	}
}
