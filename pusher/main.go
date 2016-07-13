package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
)

var taskManager *TaskManager
var rpcserver *RPCServer
var rpcclient *RPCClient

func main() {
	log.Debugf("Conf: %+v", Conf)
	rpcserver = NewRPCServer("tcp", fmt.Sprintf("%s:%d", Conf.RPCServerConf.Host, Conf.RPCServerConf.Port), "")
	rpcserver.startRpcServer()
	rpcclient = NewRPCClient("tcp", fmt.Sprintf("%s:%d", Conf.MonitorConf.Host, Conf.MonitorConf.RpcPort), "")
	rpcclient.RegisterClient(rpcserver.schema)
	tasks, err := rpcclient.GetAllTasks("")
	if err != nil {
		log.Error(err.Error())
	}
	log.Debug(tasks)
	taskManager = NewTaskManager()
	taskManager.InitTaskMap(tasks)
	taskManager.InitTasKRoutine()
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
			rpcclient.DeregisterClient(rpcserver.schema)
			return
		case syscall.SIGHUP:
			// TODO reload
			//return
		default:
			return
		}
	}
}
