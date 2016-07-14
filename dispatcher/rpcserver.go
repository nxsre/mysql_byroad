package main

import (
	"mysql_byroad/common"
	"mysql_byroad/model"
	"net"
	"net/http"
	"net/rpc"

	log "github.com/Sirupsen/logrus"
)

type RPCServer struct {
	protocol string
	schema   string
	desc     string
}

func NewRPCServer(protocol, schema, desc string) *RPCServer {
	server := RPCServer{
		protocol: protocol,
		schema:   schema,
		desc:     desc,
	}
	return &server
}

func (this *RPCServer) startRpcServer() {
	rpc.Register(this)
	rpc.HandleHTTP()
	l, e := net.Listen(this.protocol, this.schema)
	if e != nil {
		panic(e.Error())
	}
	go http.Serve(l, nil)
	log.Infof("start rpc server at %s", this.schema)
}

func (rs *RPCServer) AddTask(task *model.Task, status *string) error {
	log.Debugf("add task: %+v", task)
	*status = "sucess"
	taskManager.taskIdMap.Set(task.ID, task)
	if task.Stat == common.TASK_STATE_START {
		taskManager.notifyTaskMap.AddTask(task)
	}
	return nil
}

func (rs *RPCServer) DeleteTask(id int64, status *string) error {
	log.Debug("delete task: ", id)
	*status = "success"
	taskManager.taskIdMap.Delete(id)
	taskManager.notifyTaskMap.UpdateNotifyTaskMap(taskManager.taskIdMap)
	return nil
}

func (rs *RPCServer) UpdateTask(task *model.Task, status *string) error {
	log.Debug("update task:", task)
	*status = "success"
	taskManager.taskIdMap.Set(task.ID, task)
	taskManager.notifyTaskMap.UpdateNotifyTaskMap(taskManager.taskIdMap)
	return nil
}

func (rs *RPCServer) GetColumns(dbname string, os *model.OrderedSchemas) error {
	log.Debug("get db columns")
	*os = columnManager.GetOrderedColumns()
	return nil
}

func (rs *RPCServer) GetAllColumns(dbname string, os *model.OrderedSchemas) error {
	log.Debug("get all columns")
	*os = columnManager.GetOrderedColumns()
	return nil
}
