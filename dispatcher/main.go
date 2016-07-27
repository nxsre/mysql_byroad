package main

import (
	"fmt"
	"mysql_byroad/model"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	_ "github.com/mattn/go-sqlite3"
)

var (
	startTime         time.Time
	replicationClient *ReplicationClient
	columnManager     *ColumnManager
	taskManager       *TaskManager
	eventEnqueuer     *EventEnqueuer
	rpcClient         *RPCClient
	rpcServer         *RPCServer
	binlogStatistics  *model.BinlogStatistics
	binlogInfo        *model.BinlogInfo

	confdb *ConfigDB
)

func initGlobal() {
	var err error
	rpcClientSchema := fmt.Sprintf("%s:%d", Conf.MonitorConf.Host, Conf.MonitorConf.RpcPort)
	rpcServerSchema := fmt.Sprintf("%s:%d", Conf.RPCServerConf.Host, Conf.RPCServerConf.Port)
	rpcServer = NewRPCServer("tcp", rpcServerSchema, Conf.RPCServerConf.Desc)
	rpcServer.startRpcServer()
	rpcClient = NewRPCClient("tcp", rpcClientSchema, "")
	rpcClient.RegisterClient(rpcServer.schema, rpcServer.desc)
	columnManager = NewColumnManager(Conf.MysqlConf)
	taskManager = NewTaskManager()
	eventEnqueuer = NewEventEnqueuer(Conf.NSQConf.LookupdHttpAddrs)
	binlogStatistics = &model.BinlogStatistics{
		Statistics: make([]*model.BinlogStatistic, 0, 100),
	}
	binlogInfo = &model.BinlogInfo{}

	confdb, err = NewConfigDB()
	if err != nil {
		log.Panic(err)
	}
}

func main() {
	startTime = time.Now()
	InitLog()
	log.Debugf("Conf: %+v", Conf)
	initGlobal()
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
	go binlogTicker()
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
			rpcClient.DeregisterClient(rpcServer.schema, rpcServer.desc)
			_, err := confdb.SaveBinlogInfo()
			if err != nil {
				log.Errorf("save binlog info error: %s", err.Error())
			}
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
