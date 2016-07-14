package main

import (
	"mysql_byroad/model"

	log "github.com/Sirupsen/logrus"
)

type DispatcherManager struct {
	rpcclients []*RPCClient
	schemas    []string
}

func NewDispatcherManager() *DispatcherManager {
	dm := &DispatcherManager{
		rpcclients: make([]*RPCClient, 0, 10),
		schemas:    make([]string, 0, 10),
	}
	return dm
}

func (dm *DispatcherManager) AddDispatchClient(schema, desc string) {
	client := NewRPCClient("tcp", schema, desc)
	dm.rpcclients = append(dm.rpcclients, client)
	log.Infof("add dispatch client %s, length: %d", schema, len(dm.rpcclients))
}

func (dm *DispatcherManager) DeleteDispatchClient(schema string) {
	for idx, s := range dm.schemas {
		if s == schema {
			dm.schemas = append(dm.schemas[:idx], dm.schemas[idx+1:]...)
			break
		}
	}
	for idx, client := range dm.rpcclients {
		if client.Schema == schema {
			dm.rpcclients = append(dm.rpcclients[:idx], dm.rpcclients[idx+1:]...)
			break
		}
	}
	log.Infof("delete dispatch client %s, length: %d", schema, len(dm.rpcclients))
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
