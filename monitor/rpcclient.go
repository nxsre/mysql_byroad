package main

import (
	"mysql_byroad/model"
	"net/rpc"
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
	return
}

func (this *RPCClient) AddTask(task *model.Task) (status string, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.AddTask", task, &status)
	return
}

func (this *RPCClient) DeleteTask(task *model.Task) (status string, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.DeleteTask", task.ID, &status)
	return
}

func (this *RPCClient) UpdateTask(task *model.Task) (status string, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.UpdateTask", task, &status)
	return
}

func (this *RPCClient) GetColumns(dbname string) (dbmap model.OrderedSchemas, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.GetColumns", dbname, &dbmap)
	return
}

func (this *RPCClient) GetAllColumns() (dbmap model.OrderedSchemas, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.GetAllColumns", "", &dbmap)
	return
}

func (this *RPCClient) GetBinlogStatistics() (statics []*model.BinlogStatistic, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.GetBinlogStatistics", "", &statics)
	return
}

func (this *RPCClient) GetMasterStatus() (binfo *model.BinlogInfo, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.GetMasterStatus", "", &binfo)
	return
}

func (this *RPCClient) GetCurrentBinlogInfo() (binfo *model.BinlogInfo, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("RPCServer.GetCurrentBinlogInfo", "", &binfo)
	return
}

func (this RPCClient) GetSysStatus() (status map[string]interface{}, err error) {
	client, err := this.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Close()
	err = client.Call("RPCServer.GetStatus", "", &status)
	return
}
