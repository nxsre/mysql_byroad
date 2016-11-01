package main

import (
	"fmt"
	"mysql_byroad/model"
	"sort"
	"time"

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

func (dm *DispatcherManager) GetRPCClient(desc string) (*RPCClient, bool) {
	return dm.rpcclients.Get(desc)
}

func (dm *DispatcherManager) AddDispatchClient(schema, desc string) {
	if _, ok := dm.rpcclients.Get(desc); !ok {
		client := NewRPCClient("tcp", schema, desc)
		dm.rpcclients.Set(desc, client)
		timer := time.NewTimer(Conf.RPCClientLookupInterval.Duration)
		dm.timers.Set(desc, timer)
		go func() {
			for {
				<-timer.C
				dm.DeleteDispatchClient(desc)
			}
		}()
		log.Infof("add dispatch client %s, length: %d", desc, dm.rpcclients.Length())
	}
}

func (dm *DispatcherManager) DeleteDispatchClient(desc string) {
	dm.rpcclients.Delete(desc)
	if timer, ok := dm.timers.Get(desc); ok {
		timer.Stop()
	}
	dm.timers.Delete(desc)
	log.Infof("delete dispatch client %s, length: %d", desc, dm.rpcclients.Length())
}

func (dm *DispatcherManager) UpdateDispatchClient(schema, desc string) {
	if timer, ok := dm.timers.Get(desc); ok {
		timer.Reset(Conf.RPCClientLookupInterval.Duration)
	}
	if _, ok := dm.rpcclients.Get(desc); !ok {
		dm.AddDispatchClient(schema, desc)
	}
	log.Debugf("dispatcher manager update client %s: %s", schema, desc)
}

func (dm *DispatcherManager) AddTask(task *model.Task) error {
	if client, ok := dm.rpcclients.Get(task.DBInstanceName); ok {
		status, err := client.AddTask(task)
		if err != nil {
			log.Errorf("dispatch manager add task status: %s, error: %s", status, err.Error())
			return err
		}
	}
	return nil
}

func (dm *DispatcherManager) DeleteTask(task *model.Task) error {
	if client, ok := dm.rpcclients.Get(task.DBInstanceName); ok {
		status, err := client.DeleteTask(task)
		if err != nil {
			log.Errorf("dispatch manager delete task status: %s, error: %s", status, err.Error())
			return err
		}
	}
	return nil
}

func (dm *DispatcherManager) UpdateTask(task *model.Task) error {
	if client, ok := dm.rpcclients.Get(task.DBInstanceName); ok {
		status, err := client.UpdateTask(task)
		if err != nil {
			log.Errorf("dispatch manager update task status: %s, error: %s", status, err.Error())
			return err
		}
	}
	return nil
}

func (dm *DispatcherManager) StartTask(task *model.Task) error {
	if client, ok := dm.rpcclients.Get(task.DBInstanceName); ok {
		status, err := client.StartTask(task)
		if err != nil {
			log.Errorf("dispatch manager start task status: %s, error: %s", status, err.Error())
			return err
		}
	}
	return nil
}

func (dm *DispatcherManager) StopTask(task *model.Task) error {
	if client, ok := dm.rpcclients.Get(task.DBInstanceName); ok {
		status, err := client.StopTask(task)
		if err != nil {
			log.Errorf("dispatch manager stop task status: %s, error: %s", status, err.Error())
			return err
		}
	}
	return nil
}

func (dm *DispatcherManager) GetColumns(desc string) (dbmap model.OrderedSchemas, err error) {
	if client, ok := dm.rpcclients.Get(desc); ok {
		return client.GetAllColumns()
	}
	return nil, nil
}

func (dm *DispatcherManager) GetBinlogStatistics(desc string) (statics []*model.BinlogStatistic, err error) {
	if client, ok := dm.rpcclients.Get(desc); ok {
		return client.GetBinlogStatistics()
	}
	return nil, nil
}

func (dm *DispatcherManager) GetMasterStatus(desc string) (binfo *model.BinlogInfo, err error) {
	if client, ok := dm.rpcclients.Get(desc); ok {
		return client.GetMasterStatus()
	}
	return nil, nil
}

func (dm *DispatcherManager) GetCurrentBinlogInfo(desc string) (binfo *model.BinlogInfo, err error) {
	if client, ok := dm.rpcclients.Get(desc); ok {
		return client.GetCurrentBinlogInfo()
	}
	return nil, nil
}

func (dm *DispatcherManager) GetSysStatus(desc string) (status map[string]interface{}, err error) {
	if client, ok := dm.rpcclients.Get(desc); ok {
		return client.GetSysStatus()
	}
	return nil, nil
}

func (dm *DispatcherManager) RunBinlogCheck() {
	go func() {
		for {
			select {
			case <-time.After(Conf.AlertConfig.BinlogCheckPeriod.Duration):
				for dispatcher := range dm.rpcclients.Iter() {
					checkBinlog(dispatcher)
				}
			}
		}
	}()
}

func checkBinlog(dispatcher *RPCClient) {
	masterStatus, err := dispatcher.GetMasterStatus()
	if err != nil {
	}
	currentStatus, err := dispatcher.GetCurrentBinlogInfo()
	if err != nil {
	}
	if masterStatus.Position-currentStatus.Position > Conf.AlertConfig.BinlogPosGap {
		content := fmt.Sprintf("旁路系统\n时间：%s\n数据库实例：%s\nmaster status: %+v\ncurrent status: %+v", time.Now().String(), dispatcher.Desc, masterStatus, currentStatus)
		SendAlert(content)
	}
}
