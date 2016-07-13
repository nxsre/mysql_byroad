package main

import (
	"epg/log"
	"mysql_byroad/common"
	"mysql_byroad/model"
	"net"
	"net/http"
	"net/rpc"
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
}

func (rs *RPCServer) AddTask(task *model.Task, status *string) error {
	log.Debug("add task: ", task)
	*status = "sucess"
	if task.Stat == common.TASK_STATE_START {
		taskManager.StartTask(task)
	} else {
		taskManager.AddTask(task)
	}
	return nil
}

func (rs *RPCServer) DeleteTask(id int64, status *string) error {
	log.Debug("delete task: ", id)
	*status = "success"
	task := new(model.Task)
	task.ID = id
	taskManager.DeleteTask(task)
	return nil
}

func (rs *RPCServer) UpdateTask(task *model.Task, status *string) error {
	log.Debug("update task:", task)
	*status = "success"
	if task.Stat == common.TASK_STATE_START {
		taskManager.StartTask(task)
	} else {
		taskManager.StopTask(task)
	}
	return nil
}
