package main

import (
	"mysql_byroad/model"
	"net"
	"net/http"
	"net/rpc"
	"runtime"
	"time"

	log "github.com/Sirupsen/logrus"
)

type RPCServer struct {
	protocol string
	schema   string
	desc     string
	listener net.Listener
}

func NewRPCServer(protocol, schema, desc string) *RPCServer {
	server := RPCServer{
		protocol: protocol,
		schema:   schema,
		desc:     desc,
	}
	return &server
}

func (this *RPCServer) getSchema() string {
	return this.listener.Addr().String()
}

func (this *RPCServer) startRpcServer() {
	rpc.Register(this)
	rpc.HandleHTTP()
	l, e := net.Listen(this.protocol, this.schema)
	if e != nil {
		panic(e.Error())
	}
	go http.Serve(l, nil)
	this.listener = l
	log.Infof("start rpc server at %s", this.getSchema())
}

func (rs *RPCServer) AddTask(task *model.Task, status *string) error {
	log.Infof("rpc add task: %+v", task)
	*status = "sucess"
	dispatcher.taskManager.taskIdMap.Set(task.ID, task)
	if task.Stat == model.TASK_STATE_START {
		dispatcher.taskManager.notifyTaskMap.AddTask(task)
	}
	return nil
}

func (rs *RPCServer) DeleteTask(id int64, status *string) error {
	log.Info("rpc delete task: ", id)
	*status = "success"
	dispatcher.taskManager.taskIdMap.Delete(id)
	dispatcher.taskManager.notifyTaskMap.UpdateNotifyTaskMap(dispatcher.taskManager.taskIdMap)
	return nil
}

func (rs *RPCServer) UpdateTask(task *model.Task, status *string) error {
	log.Infof("rpc update task: %+v", task)
	*status = "success"
	dispatcher.taskManager.taskIdMap.Set(task.ID, task)
	dispatcher.taskManager.notifyTaskMap.UpdateNotifyTaskMap(dispatcher.taskManager.taskIdMap)
	return nil
}

func (rs *RPCServer) StartTask(task *model.Task, status *string) error {
	log.Infof("rpc start task: %+v", task)
	*status = "success"
	dispatcher.taskManager.taskIdMap.Set(task.ID, task)
	dispatcher.taskManager.notifyTaskMap.UpdateNotifyTaskMap(dispatcher.taskManager.taskIdMap)
	return nil
}

func (rs *RPCServer) StopTask(task *model.Task, status *string) error {
	log.Infof("rpc stop task: %+v", task)
	*status = "success"
	dispatcher.taskManager.taskIdMap.Set(task.ID, task)
	dispatcher.taskManager.notifyTaskMap.UpdateNotifyTaskMap(dispatcher.taskManager.taskIdMap)
	return nil
}

func (rs *RPCServer) GetColumns(dbname string, os *model.OrderedSchemas) error {
	log.Info("rpc get db columns")
	*os = dispatcher.replicationClient.columnManager.GetOrderedColumns()
	return nil
}

func (rs *RPCServer) GetAllColumns(dbname string, os *model.OrderedSchemas) error {
	log.Info("rpc get all columns")
	*os = dispatcher.replicationClient.columnManager.GetOrderedColumns()
	return nil
}

func (rs *RPCServer) GetBinlogStatistics(username string, statics *[]*model.BinlogStatistic) error {
	log.Info("rpc get binlog statistics")
	*statics = dispatcher.binlogStatistics.Statistics
	return nil
}

func (rs *RPCServer) GetStatus(username string, st *map[string]interface{}) error {
	log.Info("rpc get status")
	start := dispatcher.startTime
	duration := time.Now().Sub(start)
	statusMap := make(map[string]interface{})
	statusMap["Start"] = start.String()
	statusMap["Duration"] = duration.String()
	statusMap["routineNumber"] = runtime.NumGoroutine()
	*st = statusMap
	return nil
}

func (rs *RPCServer) GetMasterStatus(username string, binfo *model.BinlogInfo) error {
	log.Info("rpc get master status")
	info, err := GetMasterStatus(Conf.MysqlConf)
	*binfo = *info
	return err
}

func (rs *RPCServer) GetCurrentBinlogInfo(username string, binfo *model.BinlogInfo) error {
	log.Info("rpc get current binlog info")
	*binfo = *(dispatcher.replicationClient.binlogInfo)
	return nil
}
