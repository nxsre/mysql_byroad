package main

import (
	"fmt"
	"mysql_byroad/model"
	"time"
)

type Dispatcher struct {
	startTime         time.Time
	replicationClient *ReplicationClient
	rpcClient         *RPCClient
	rpcServer         *RPCServer
	binlogStatistics  *model.BinlogStatistics
}

func NewDispatcher() *Dispatcher {
	rpcClientSchema := fmt.Sprintf("%s:%d", Conf.MonitorConf.Host, Conf.MonitorConf.RpcPort)
	rpcServerSchema := fmt.Sprintf("%s:%d", Conf.RPCServerConf.Host, Conf.RPCServerConf.Port)
	rpcServer := NewRPCServer("tcp", rpcServerSchema, Conf.RPCServerConf.Desc)
	rpcClient := NewRPCClient("tcp", rpcClientSchema, "")
	binlogStatistics := &model.BinlogStatistics{
		Statistics: make([]*model.BinlogStatistic, 0, 100),
	}

	//TODO: 多个mysql实例，遍历生成columnManager 和 replication client
	replicationClient := NewReplicationClient(Conf.MysqlConf)
	handler := NewRowsEventHandler(replicationClient)
	replicationClient.AddHandler(handler)

	dispatcher := &Dispatcher{}
	dispatcher.startTime = time.Now()
	dispatcher.replicationClient = replicationClient
	dispatcher.rpcClient = rpcClient
	dispatcher.rpcServer = rpcServer
	dispatcher.binlogStatistics = binlogStatistics
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
