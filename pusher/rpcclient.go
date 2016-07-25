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
	if err != nil {
		log.Errorf("rpc get client error: %s", err.Error())
	}
	return
}

func (this *RPCClient) GetAllTasks(username string) (tasks []*model.Task, err error) {
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

func (this *RPCClient) RegisterClient(schema, desc string) (status string, err error) {
	client, err := this.GetClient()
	if err != nil {
		this.pingLookup(schema, desc)
		return
	}
	defer client.Close()
	ss := ServiceSignal{
		Code:   "1",
		Schema: schema,
		Desc:   desc,
	}
	err = client.Call("Monitor.HandlePushClientSignal", ss, &status)
	if err != nil {
		log.Errorf("rpc register client error: %s", err.Error())
	}
	this.pingLookup(schema, desc)
	log.Info("rpc register client")
	return
}

func (this *RPCClient) DeregisterClient(schema, desc string) (status string, err error) {
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
	err = client.Call("Monitor.HandlePushClientSignal", ss, &status)
	if err != nil {
		log.Errorf("rpc deregister error: %s", err.Error())
	}
	log.Info("rpc deregister client")
	return
}

func (this *RPCClient) Ping(schema, desc string) (status string, err error) {
	c	lient, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	ss := ServiceSignal{
		Code:   "2",
		Schema: schema,
		Desc:   desc,
	}
	err = client.Call("Monitor.HandlePushClientSignal", ss, &status)
	if err != nil {
		log.Errorf("rpc ping error: %s", err.Error())
	}
	log.Debug("rpc ping")
	return
}

func (this *RPCClient) pingLookup(schema, desc string) {
	go func() {
		for {
			this.Ping(schema, desc)
			time.Sleep(Conf.RPCPingInterval.Duration)
		}
	}()
}
