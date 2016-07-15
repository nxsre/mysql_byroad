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

type ServiceSignal struct {
	Code   string
	Schema string
	Desc   string
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
	return
}

func (this *RPCClient) GetAllTasks(username string) (tasks []*model.Task, err error) {
	log.Info("rpc client get all tasks")
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("Monitor.GetAllTasks", username, &tasks)
	return
}

func (this *RPCClient) RegisterClient(schema, desc string) (status string, err error) {
	log.Info("rpc register client")
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	ss := ServiceSignal{
		Code:   "1",
		Schema: schema,
		Desc:   desc,
	}
	err = client.Call("Monitor.HandleDispatchClientSignal", ss, &status)
	return
}

func (this *RPCClient) DeregisterClient(schema, desc string) (status string, err error) {
	log.Info("rpc deregister client")
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	ss := ServiceSignal{
		Code:   "0",
		Schema: schema,
		Desc:   desc,
	}
	err = client.Call("Monitor.HandleDispatchClientSignal", ss, &status)
	return
}

func (this *RPCClient) Ping(schema, desc string) (status string, err error) {
	log.Info("rpc ping")
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	ss := ServiceSignal{
		Code:   "2",
		Schema: schema,
		Desc:   desc,
	}
	err = client.Call("Monitor.HandleDispatchClientSignal", ss, &status)
	return
}
