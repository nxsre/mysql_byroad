package main

import (
	"fmt"
	"mysql_byroad/model"
	"time"

	"golang.org/x/net/context"
)

type Dispatcher struct {
	startTime         time.Time
	replicationClient *ReplicationClient
	rpcClient         *RPCClient
	rpcServer         *RPCServer
	binlogStatistics  *model.BinlogStatistics
	taskManager       *TaskManager
	Config            *Config
}

func NewDispatcher(config *Config) *Dispatcher {
	dispatcher := &Dispatcher{
		Config: config,
	}
	dispatcher.startTime = time.Now()
	ctx := context.WithValue(context.Background(), "dispatcher", dispatcher)

	rpcClientSchema := fmt.Sprintf("%s:%d", config.MonitorConf.Host, config.MonitorConf.RpcPort)
	rpcServerSchema := fmt.Sprintf("%s:%d", config.RPCServerConf.Host, config.RPCServerConf.Port)
	rpcServer := NewRPCServer(ctx, "tcp", rpcServerSchema, config.RPCServerConf.Desc)
	rpcClient := NewRPCClient("tcp", rpcClientSchema, "")
	dispatcher.rpcClient = rpcClient
	dispatcher.rpcServer = rpcServer
	binlogStatistics := &model.BinlogStatistics{
		Statistics: make([]*model.BinlogStatistic, 0, 100),
	}
	dispatcher.binlogStatistics = binlogStatistics
	taskManager := NewTaskManager(ctx)
	dispatcher.taskManager = taskManager

	//TODO: 多个mysql实例，遍历生成columnManager 和 replication client
	replicationClient := NewReplicationClient(ctx)
	dispatcher.replicationClient = replicationClient
	handler := NewRowsEventHandler(ctx)
	replicationClient.AddHandler(handler)
	return dispatcher
}

func (d *Dispatcher) Start() {
	d.rpcServer.startRpcServer()
	d.rpcClient.RegisterClient(d.rpcServer.getSchema(), d.rpcServer.desc)
	d.replicationClient.Start()
}

func (d *Dispatcher) IncStatistic(schema, table, event string) {
	d.binlogStatistics.IncStatistic(schema, table, event)
}

func (d *Dispatcher) Stop() {
	d.rpcClient.DeregisterClient(d.rpcServer.getSchema(), d.rpcServer.desc)
	d.replicationClient.Stop()
	<-d.replicationClient.StopChan
	d.replicationClient.SaveBinlog()
}
