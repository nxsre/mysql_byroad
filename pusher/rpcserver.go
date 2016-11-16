package main

import (
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
	this.listener = l
	log.Infof("start rpc server at %s", this.getSchema())
	go http.Serve(l, nil)
}

func (rs *RPCServer) AddTask(task *model.Task, status *string) error {
	log.Infof("add task: %+v", task)
	*status = "sucess"
	if task.Stat == model.TASK_STATE_START {
		taskManager.StartTask(task)
	} else {
		taskManager.AddTask(task)
	}
	return nil
}

func (rs *RPCServer) DeleteTask(task *model.Task, status *string) error {
	log.Infof("delete task: %+v", task)
	*status = "success"
	taskManager.DeleteTask(task)
	return nil
}

func (rs *RPCServer) UpdateTask(task *model.Task, status *string) error {
	log.Infof("update task: %+v", task)
	*status = "success"
	taskManager.UpdateTask(task)
	return nil
}

func (rs *RPCServer) StartTask(task *model.Task, status *string) error {
	log.Infof("start task: %+v", task)
	*status = "success"
	taskManager.StartTask(task)
	return nil
}

func (rs *RPCServer) StopTask(task *model.Task, status *string) error {
	log.Infof("stop task: %+v", task)
	*status = "success"
	taskManager.StopTask(task)
	return nil
}
