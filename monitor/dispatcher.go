package main

import (
	"mysql_byroad/model"
	"time"

	"sort"

	log "github.com/Sirupsen/logrus"
)

type DispatcherManager struct {
	rpcclients *RPCClientMap
	timers     *TimerMap
}

func NewDispatcherManager() *DispatcherManager {
	dm := &DispatcherManager{
		rpcclients: NewRPCClientMap(),
		timers:     NewTimerMap(),
	}
	return dm
}

type OrderRPCClients []*RPCClient

func (o OrderRPCClients) Len() int {
	return len(o)
}

func (o OrderRPCClients) Less(i, j int) bool {
	return o[i].Desc < o[j].Desc
}

func (o OrderRPCClients) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}

func (dm *DispatcherManager) GetRPCClients() []*RPCClient {
	clients := make([]*RPCClient, 0, 10)
	for c := range dm.rpcclients.Iter() {
		clients = append(clients, c)
	}
	sort.Sort(OrderRPCClients(clients))
	return clients
}

func (dm *DispatcherManager) GetRPCClient(schema string) (*RPCClient, bool) {
	return dm.rpcclients.Get(schema)
}

func (dm *DispatcherManager) AddDispatchClient(schema, desc string) {
	if _, ok := dm.rpcclients.Get(schema); !ok {
		client := NewRPCClient("tcp", schema, desc)
		dm.rpcclients.Set(schema, client)
		timer := time.NewTimer(Conf.RPCClientLookupInterval.Duration)
		dm.timers.Set(schema, timer)
		go func() {
			for {
				<-timer.C
				dm.DeleteDispatchClient(schema)
			}
		}()
		log.Infof("add dispatch client %s, length: %d", schema, dm.rpcclients.Length())
	}
}

func (dm *DispatcherManager) DeleteDispatchClient(schema string) {
	dm.rpcclients.Delete(schema)
	if timer, ok := dm.timers.Get(schema); ok {
		timer.Stop()
	}
	dm.timers.Delete(schema)
	log.Infof("delete dispatch client %s, length: %d", schema, dm.rpcclients.Length())
}

func (dm *DispatcherManager) UpdateDispatchClient(schema, desc string) {
	if timer, ok := dm.timers.Get(schema); ok {
		timer.Reset(Conf.RPCClientLookupInterval.Duration)
	}
	if _, ok := dm.rpcclients.Get(schema); !ok {
		dm.AddDispatchClient(schema, desc)
	}
	log.Debugf("dispatcher manager update client %s: %s", schema, desc)
}

func (dm *DispatcherManager) AddTask(task *model.Task) {
	for client := range dm.rpcclients.Iter() {
		status, err := client.AddTask(task)
		if err != nil {
			log.Errorf("dispatch manager add task status: %s, error: %s", status, err.Error())
		}
	}
}

func (dm *DispatcherManager) DeleteTask(task *model.Task) {
	for client := range dm.rpcclients.Iter() {
		status, err := client.DeleteTask(task)
		if err != nil {
			log.Errorf("dispatch manager delete task status: %s, error: %s", status, err.Error())
		}
	}
}

func (dm *DispatcherManager) UpdateTask(task *model.Task) {
	for client := range dm.rpcclients.Iter() {
		status, err := client.UpdateTask(task)
		if err != nil {
			log.Errorf("dispatch manager update task status: %s, error: %s", status, err.Error())
		}
	}
}

func (dm *DispatcherManager) StartTask(task *model.Task) {
	for client := range dm.rpcclients.Iter() {
		status, err := client.StartTask(task)
		if err != nil {
			log.Errorf("dispatch manager start task status: %s, error: %s", status, err.Error())
		}
	}
}

func (dm *DispatcherManager) StopTask(task *model.Task) {
	for client := range dm.rpcclients.Iter() {
		status, err := client.StopTask(task)
		if err != nil {
			log.Errorf("dispatch manager stop task status: %s, error: %s", status, err.Error())
		}
	}
}

func (dm *DispatcherManager) GetColumns(schema string) (dbmap model.OrderedSchemas, err error) {
	for client := range dm.rpcclients.Iter() {
		if client.Schema == schema {
			return client.GetAllColumns()
		}
	}
	return nil, nil
}

func (dm *DispatcherManager) GetBinlogStatistics(schema string) (statics []*model.BinlogStatistic, err error) {
	for client := range dm.rpcclients.Iter() {
		if client.Schema == schema {
			return client.GetBinlogStatistics()
		}
	}
	return nil, nil
}

func (dm *DispatcherManager) GetMasterStatus(schema string) (binfo *model.BinlogInfo, err error) {
	for client := range dm.rpcclients.Iter() {
		if client.Schema == schema {
			return client.GetMasterStatus()
		}
	}
	return nil, nil
}

func (dm *DispatcherManager) GetCurrentBinlogInfo(schema string) (binfo *model.BinlogInfo, err error) {
	for client := range dm.rpcclients.Iter() {
		if client.Schema == schema {
			return client.GetCurrentBinlogInfo()
		}
	}
	return nil, nil
}

func (dm *DispatcherManager) GetSysStatus(schema string) (status map[string]interface{}, err error) {
	for client := range dm.rpcclients.Iter() {
		if client.Schema == schema {
			return client.GetSysStatus()
		}
	}
	return nil, nil
}
