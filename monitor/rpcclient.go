package main

import (
	"mysql_byroad/model"
	"net/rpc"

	log "github.com/Sirupsen/logrus"
)

type RPCClient struct {
	protocol string
	Schema   string
	Desc     string
}

func NewRPCClient(protocol, schema, desc string) *RPCClient {
	client := RPCClient{
		protocol: protocol,
		Schema:   schema,
		Desc:     desc,
	}

	return &client
}

func (this *RPCClient) GetClient() (client *rpc.Client, err error) {
	client, err = rpc.DialHTTP(this.protocol, this.Schema)
	if err != nil {
		log.Errorf("rpc get client error: %s", err.Error())
	}
	return
}

func (this *RPCClient) AddTask(task *model.Task) (status string, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.AddTask", task, &status)
	if err != nil {
		log.Errorf("rpc add task error: %s", err.Error())
	}
	return
}

func (this *RPCClient) DeleteTask(task *model.Task) (status string, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.DeleteTask", task, &status)
	if err != nil {
		log.Errorf("rpc delete task error: %s", err.Error())
	}
	return
}

func (this *RPCClient) UpdateTask(task *model.Task) (status string, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.UpdateTask", task, &status)
	if err != nil {
		log.Errorf("rpc update task error: %s", err.Error())
	}
	return
}

func (this *RPCClient) StartTask(task *model.Task) (status string, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.StartTask", task, &status)
	if err != nil {
		log.Errorf("rpc start task error: %s", err.Error())
	}
	return
}

func (this *RPCClient) StopTask(task *model.Task) (status string, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.StopTask", task, &status)
	if err != nil {
		log.Errorf("rpc stop task error: %s", err.Error())
	}
	return
}
