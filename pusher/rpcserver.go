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
	_, err := taskManager.AddTask(task)
	if err != nil {
		return err
	}
	if task.Stat == model.TASK_STATE_START {
		return taskManager.StartTask(task)
	}
	return nil
}

func (rs *RPCServer) DeleteTask(task *model.Task, status *string) error {
	log.Infof("delete task: %s", task.Name)
	*status = "success"
	_, err := taskManager.DeleteTask(task)
	return err
}

func (rs *RPCServer) UpdateTask(task *model.Task, status *string) error {
	log.Infof("update task: %+v", task)
	*status = "success"
	_, err := taskManager.UpdateTask(task)
	return err
}

func (rs *RPCServer) StartTask(task *model.Task, status *string) error {
	log.Infof("start task: %+v", task)
	*status = "success"
	// 确保任务已经停止
	err := taskManager.StopTask(task)
	if err != nil {
		return err
	}
	_, err = taskManager.AddTask(task)
	if err != nil {
		return err
	}
	return taskManager.StartTask(task)
}

func (rs *RPCServer) StopTask(task *model.Task, status *string) error {
	log.Infof("stop task: %+v", task)
	*status = "success"
	err := taskManager.StopTask(task)
	return err
}
