package main

import (
	"fmt"
	"mysql_byroad/model"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mysql_byroad/nsq"

	log "github.com/Sirupsen/logrus"
	"github.com/jmoiron/sqlx"
)

var (
	pusherManager     *PusherManager
	dispatcherManager *DispatcherManager
	rpcServer         *Monitor
	nsqManager        *nsqm.NSQManager
)

func main() {
	var err error
	log.Debugf("Conf: %+v", Conf)
	pusherManager = NewPusherManager()
	dispatcherManager = NewDispatcherManager()
	rpcServer = NewRPCServer("tcp", fmt.Sprintf("%s:%d", Conf.RPCServerConf.Host, Conf.RPCServerConf.Port), "")
	rpcServer.start()
	nsqManager, err = nsqm.NewNSQManager(Conf.NSQLookupdAddress)
	if err != nil {
		log.Error("new nsq manager error: ", err.Error())
	}
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=true",
		Conf.MysqlConf.Username, Conf.MysqlConf.Password, Conf.MysqlConf.Host, Conf.MysqlConf.Port,
		Conf.MysqlConf.DBName)
	confdb, err := sqlx.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	model.Init(confdb)
	go StartServer()
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
