package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
)

var (
	replicationClient *ReplicationClient
	columnManager     *ColumnManager
	taskManager       *TaskManager
	eventEnqueuer     *EventEnqueuer
)

func initGolbal(conf Config) {
	columnManager = NewColumnManager(conf.MysqlConf)
	taskManager = NewTaskManager()
	eventEnqueuer = NewEventEnqueuer(Conf.NSQConf.LookupdHttpAddrs)
}

func main() {
	log.Debugf("Conf: %+v", Conf)
	initGolbal(Conf)
	conf := Conf.MysqlConf
	handler := NewRowsEventHandler(conf)
	replicationClient = &ReplicationClient{
		ServerId:       conf.ServerId,
		Host:           conf.Host,
		Port:           conf.Port,
		Username:       conf.Username,
		Password:       conf.Password,
		BinlogFilename: conf.BinlogFilename,
		BinlogPosition: conf.BinlogPosition,
		StopChan:       make(chan bool, 1),
	}
	replicationClient.AddHandler(handler)
	replicationClient.Start()
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
			replicationClient.Stop()
			taskManager.rpcClient.DeregisterClient(taskManager.rpcServer.schema, taskManager.rpcServer.desc)
			<-replicationClient.StopChan
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
