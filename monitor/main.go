package main

import (
	"fmt"
	"mysql_byroad/model"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/jmoiron/sqlx"
)

var pusherManager *PusherManager
var rpcClient *RPCClient
var rpcServer *Monitor

func main() {
	log.Debugf("Conf: %+v", Conf)
	pusherManager = NewPusherManager()
	rpcClient = NewRPCClient("tcp", fmt.Sprintf("%s:%d", Conf.DispatcherConf.Host, Conf.DispatcherConf.RPCPort), "")
	rpcServer = NewRPCServer("tcp", fmt.Sprintf("%s:%d", Conf.RPCServerConf.Host, Conf.RPCServerConf.Port), "")
	rpcServer.start()
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=true",
		Conf.MysqlConf.Username, Conf.MysqlConf.Password, Conf.MysqlConf.Host, Conf.MysqlConf.Port,
		Conf.MysqlConf.DBName)
	confdb, err := sqlx.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	model.Init(confdb)
	//for test
	time.AfterFunc(time.Second*10, func() {
		task := &model.Task{
			ID:   4,
			Name: "yangxin",
		}
		status, err := rpcClient.AddTask(task)
		if err != nil {
			log.Debug("rpcclient add task ", status, err)
		}
		pusherManager.AddTask(task)
	})

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
