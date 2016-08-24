package main

import (
	"mysql_byroad/model"
	"net/rpc"
	"time"

	log "github.com/Sirupsen/logrus"
)

type RPCClient struct {
	protocol string
	Schema   string
}

type ServiceSignal struct {
	Code   string
	Schema string
	Desc   string
}

func NewRPCClient(schema string) *RPCClient {
	client := RPCClient{
		protocol: "tcp",
		Schema:   schema,
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

func (this *RPCClient) GetAllTasks(username string) (tasks []*model.Task, err error) {
	log.Info("rpc client get all tasks")
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("Monitor.GetAllTasks", username, &tasks)
	if err != nil {
		log.Errorf("rpc get all tasks error: %s", err.Error())
	}
	return
}

func (this *RPCClient) GetTasks(dbname string) (tasks []*model.Task, err error) {
	log.Info("rpc client get tasks")
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("Monitor.GetTaskByInstanceName", dbname, &tasks)
	if err != nil {
		log.Errorf("rpc get tasks error: %s", err.Error())
	}
	return
}

func (this *RPCClient) RegisterClient(schema, desc string, interval time.Duration) (status string, err error) {
	log.Info("rpc register client")
	client, err := this.GetClient()
	if err != nil {
		this.PingLoop(schema, desc, interval)
		return
	}
	defer client.Close()
	ss := ServiceSignal{
		Code:   "1",
		Schema: schema,
		Desc:   desc,
	}
	err = client.Call("Monitor.HandleDispatchClientSignal", ss, &status)
	if err != nil {
		log.Errorf("register rpc client error: %s", err.Error())
	}
	this.PingLoop(schema, desc, interval)
	return
}

func (this *RPCClient) PingLoop(schema, desc string, interval time.Duration) {
	go func() {
		for {
			this.Ping(schema, desc)
			time.Sleep(interval)
		}
	}()
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
	if err != nil {
		log.Errorf("rpc deregister error: %s", err.Error())
	}
	return
}

func (this *RPCClient) Ping(schema, desc string) (status string, err error) {
	log.Debug("rpc ping")
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
	if err != nil {
		log.Errorf("rpc ping error: %s", err.Error())
	}
	return
}
