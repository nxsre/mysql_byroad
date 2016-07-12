package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
)

var replicationClients = make([]*ReplicationClient, 0, 5)

func main() {
	log.Debugf("Conf: %+v", Conf)
	for _, conf := range Conf.MysqlConfs {
		handler := NewRowsEventHandler(conf)
		client := &ReplicationClient{
			ServerId:       conf.ServerId,
			Host:           conf.Host,
			Port:           conf.Port,
			Username:       conf.Username,
			Password:       conf.Password,
			BinlogFilename: conf.BinlogFilename,
			BinlogPosition: conf.BinlogPosition,
			StopChan:       make(chan bool, 1),
		}
		client.AddHandler(handler)
		client.Start()
		replicationClients = append(replicationClients, client)
	}
	HandleSignal()
}

// HandleSignal fetch signal from chan then do exit or reload.
func HandleSignal() {
	// Block until a signal is received.
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT, syscall.SIGSTOP)
	for {
		s := <-c
		log.Infof("get a signal %s", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGINT:
			for _, client := range replicationClients {
				client.Stop()
				<-client.StopChan
			}
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
