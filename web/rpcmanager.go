package web

import (
	"container/list"
	"encoding/json"
	"io/ioutil"
	"mysql_byroad/common"
	"net"
)

type RPCClientManager struct {
	clients *list.List
}

func NewRPCClientManager() *RPCClientManager {
	configer, err := common.NewConfiger(configFile)
	if err != nil {
		panic(err.Error())
	}
	manager := RPCClientManager{
		clients: list.New(),
	}
	clientsConfigs := configer.GetRPCClients()
	for _, cc := range clientsConfigs {
		manager.AddClient(cc.Schema, cc.Desc)
	}

	return &manager
}

func (this *RPCClientManager) GetClient(schema string) *RPCClient {
	var client *RPCClient
	for e := this.clients.Front(); e != nil; e = e.Next() {
		client = e.Value.(*RPCClient)
		if client.Schema == schema {
			return client
		}
	}
	return nil
}

func (this *RPCClientManager) GetClients() []*RPCClient {
	cls := make([]*RPCClient, 0, 10)
	for e := this.clients.Front(); e != nil; e = e.Next() {
		cls = append(cls, e.Value.(*RPCClient))
	}
	return cls
}

func (this *RPCClientManager) AddClient(schema string, desc string) {
	rpcclient := NewRPCClient("tcp", schema, desc)
	this.clients.PushBack(rpcclient)
}

func (this *RPCClientManager) RemoveClient(schema string) {
	var client *RPCClient
	for e := this.clients.Front(); e != nil; e = e.Next() {
		client = e.Value.(*RPCClient)
		if client.Schema == schema {
			this.clients.Remove(e)
			break
		}
	}
}

type ServiceSignal struct {
	Code   string
	Schema string
	Desc   string
}

func (this *RPCClientManager) HandleSignal(service string) error {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}
			go this.handleClient(conn)
		}
	}()
	return nil
}

func (this *RPCClientManager) handleClient(conn net.Conn) error {
	message, err := ioutil.ReadAll(conn)
	if err != nil {
		return err
	}
	ss := new(ServiceSignal)
	json.Unmarshal(message, &ss)
	if ss.Code == "0" {
		this.AddClient(ss.Schema, ss.Desc)
	} else if ss.Code == "1" {
		this.RemoveClient(ss.Schema)
	}
	return nil
}
