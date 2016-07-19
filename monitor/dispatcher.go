package main

import (
	"mysql_byroad/model"
	"time"

	log "github.com/Sirupsen/logrus"
)

type DispatcherManager struct {
	rpcclients map[string]*RPCClient
	timers     map[string]*time.Timer
}

func NewDispatcherManager() *DispatcherManager {
	dm := &DispatcherManager{
		rpcclients: make(map[string]*RPCClient, 10),
		timers:     make(map[string]*time.Timer, 10),
	}
	return dm
}

func (dm *DispatcherManager) GetRPCClients() []*RPCClient {
	clients := make([]*RPCClient, 0, 10)
	for _, c := range dm.rpcclients {
		clients = append(clients, c)
	}
	return clients
}

func (dm *DispatcherManager) GetRPCClient(schema string) (*RPCClient, bool) {
	client, ok := dm.rpcclients[schema]
	return client, ok
}

func (dm *DispatcherManager) AddDispatchClient(schema, desc string) {
	if _, ok := dm.rpcclients[schema]; !ok {
		client := NewRPCClient("tcp", schema, desc)
		dm.rpcclients[schema] = client
		timer := time.NewTimer(Conf.RPCClientLookupInterval.Duration)
		dm.timers[schema] = timer
		go func() {
			for {
				<-timer.C
				dm.DeleteDispatchClient(schema)
			}
		}()
		log.Infof("add dispatch client %s, length: %d", schema, len(dm.rpcclients))
	}
}

func (dm *DispatcherManager) DeleteDispatchClient(schema string) {
	delete(dm.rpcclients, schema)
	if timer, ok := dm.timers[schema]; ok {
		timer.Stop()
	}
	delete(dm.timers, schema)
	log.Infof("delete dispatch client %s, length: %d", schema, len(dm.rpcclients))
}

func (dm *DispatcherManager) UpdateDispatchClient(schema, desc string) {
	if timer, ok := dm.timers[schema]; ok {
		timer.Reset(Conf.RPCClientLookupInterval.Duration)
	}
	if _, ok := dm.rpcclients[schema]; !ok {
		dm.AddDispatchClient(schema, desc)
	}
	log.Debugf("dispatcher manager update client %s: %s", schema, desc)
}

func (dm *DispatcherManager) AddTask(task *model.Task) {
	for _, client := range dm.rpcclients {
		status, err := client.AddTask(task)
		if err != nil {
			log.Errorf("dispatch manager add task status: %s, error: %s", status, err.Error())
		}
	}
}

func (dm *DispatcherManager) DeleteTask(task *model.Task) {
	for _, client := range dm.rpcclients {
		status, err := client.DeleteTask(task)
		if err != nil {
			log.Errorf("dispatch manager delete task status: %s, error: %s", status, err.Error())
		}
	}
}

func (dm *DispatcherManager) UpdateTask(task *model.Task) {
	for _, client := range dm.rpcclients {
		status, err := client.UpdateTask(task)
		if err != nil {
			log.Errorf("dispatch manager update task status: %s, error: %s", status, err.Error())
		}
	}
}

func (dm *DispatcherManager) GetColumns(schema string) (dbmap model.OrderedSchemas, err error) {
	for _, client := range dm.rpcclients {
		if client.Schema == schema {
			return client.GetAllColumns()
		}
	}
	return nil, nil
}

func (dm *DispatcherManager) GetBinlogStatistics(schema string) (statics []*model.BinlogStatistic, err error) {
	for _, client := range dm.rpcclients {
		if client.Schema == schema {
			return client.GetBinlogStatistics()
		}
	}
	return nil, nil
}

func (dm *DispatcherManager) GetMasterStatus(schema string) (binfo *model.BinlogInfo, err error) {
	for _, client := range dm.rpcclients {
		if client.Schema == schema {
			return client.GetMasterStatus()
		}
	}
	return nil, nil
}

func (dm *DispatcherManager) GetCurrentBinlogInfo(schema string) (binfo *model.BinlogInfo, err error) {
	for _, client := range dm.rpcclients {
		if client.Schema == schema {
			return client.GetCurrentBinlogInfo()
		}
	}
	return nil, nil
}

func (dm *DispatcherManager) GetSysStatus(schema string) (status map[string]interface{}, err error) {
	for _, client := range dm.rpcclients {
		if client.Schema == schema {
			return client.GetSysStatus()
		}
	}
	return nil, nil
}
