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
	err = client.Call("RPCServer.DeleteTask", task.ID, &status)
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
		log.Error("rpc update task error: %s", err.Error())
	}
	return
}

func (this *RPCClient) GetColumns(dbname string) (dbmap model.OrderedSchemas, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.GetColumns", dbname, &dbmap)
	if err != nil {
		log.Errorf("rpc get columns error: %s", err.Error())
	}
	return
}

func (this *RPCClient) GetAllColumns() (dbmap model.OrderedSchemas, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.GetAllColumns", "", &dbmap)
	if err != nil {
		log.Errorf("rpc get all columns error: %s", err.Error())
	}
	return
}

func (this *RPCClient) GetBinlogStatistics() (statics []*model.BinlogStatistic, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.GetBinlogStatistics", "", &statics)
	if err != nil {
		log.Errorf("rpc get binlog statistics error: %s", err.Error())
	}
	return
}

func (this *RPCClient) GetMasterStatus() (binfo *model.BinlogInfo, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.GetMasterStatus", "", &binfo)
	if err != nil {
		log.Errorf("rpc get master status error: %s", err.Error())
	}
	return
}

func (this *RPCClient) GetCurrentBinlogInfo() (binfo *model.BinlogInfo, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.GetCurrentBinlogInfo", "", &binfo)
	if err != nil {
		log.Errorf("rpc get current binlog info error: %s", err.Error())
	}
	return
}

func (this RPCClient) GetSysStatus() (status map[string]interface{}, err error) {
	client, err := this.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Close()
	err = client.Call("RPCServer.GetStatus", "", &status)
	if err != nil {
		log.Errorf("rpc get system status error: %s", err.Error())
	}
	return
}
