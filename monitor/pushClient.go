package main

import (
	"mysql_byroad/model"
	"time"

	log "github.com/Sirupsen/logrus"
)

type PusherManager struct {
	rpcclients map[string]*RPCClient
	timers     map[string]*time.Timer
}

func NewPusherManager() *PusherManager {
	pm := &PusherManager{
		rpcclients: make(map[string]*RPCClient, 10),
		timers:     make(map[string]*time.Timer, 10),
	}
	return pm
}

func (pm *PusherManager) GetPushClient(schema string) (*RPCClient, bool) {
	client, ok := pm.rpcclients[schema]
	return client, ok
}

func (pm *PusherManager) AddPushClient(schema, desc string) {
	if _, ok := pm.rpcclients[schema]; !ok {
		client := NewRPCClient("tcp", schema, desc)
		pm.rpcclients[schema] = client
		timer := time.NewTimer(Conf.RPCClientLookupInterval.Duration)
		pm.timers[schema] = timer
		go func() {
			for {
				<-timer.C
				pm.DeletePushClient(schema)
			}
		}()
	}

	log.Infof("add push client: %s, length: %d ", schema, len(pm.rpcclients))
}

func (pm *PusherManager) DeletePushClient(schema string) {
	delete(pm.rpcclients, schema)
	if timer, ok := pm.timers[schema]; ok {
		timer.Stop()
	}
	delete(pm.timers, schema)
	log.Infof("delete push client %s, length: %d", schema, len(pm.rpcclients))
}

func (pm *PusherManager) UpdatePushClient(schema, desc string) {
	if timer, ok := pm.timers[schema]; ok {
		timer.Reset(Conf.RPCClientLookupInterval.Duration)
	}
	if _, ok := pm.rpcclients[schema]; !ok {
		pm.AddPushClient(schema, desc)
	}
}

func (pm *PusherManager) AddTask(task *model.Task) {
	for _, client := range pm.rpcclients {
		status, err := client.AddTask(task)
		if err != nil {
			log.Errorf("pusher manager add task status: %s, error: %s", status, err.Error())
		}
	}
}

func (pm *PusherManager) DeleteTask(task *model.Task) {
	for _, client := range pm.rpcclients {
		status, err := client.DeleteTask(task)
		if err != nil {
			log.Errorf("pusher manager delete task status: %s, error: %s", status, err.Error())
		}
	}
}

func (pm *PusherManager) UpdateTask(task *model.Task) {
	for _, client := range pm.rpcclients {
		status, err := client.UpdateTask(task)
		if err != nil {
			log.Errorf("pusher manager update task status: %s, error: %s", status, err.Error())
		}
	}
}
