package main

import (
	"fmt"
	"mysql_byroad/model"
	"time"

	log "github.com/Sirupsen/logrus"
)

type PusherManager struct {
	rpcclients *RPCClientMap
	timers     *TimerMap
}

func NewPusherManager() *PusherManager {
	pm := &PusherManager{
		rpcclients: NewRPCClientMap(),
		timers:     NewTimerMap(),
	}
	return pm
}

func (pm *PusherManager) GetPushClient(schema string) (*RPCClient, bool) {
	return pm.rpcclients.Get(schema)
}

func (pm *PusherManager) GetPushClients() []*RPCClient {
	clients := []*RPCClient{}
	for client := range pm.rpcclients.Iter() {
		clients = append(clients, client)
	}
	return clients
}

func (pm *PusherManager) AddPushClient(schema, desc string) {
	if _, ok := pm.rpcclients.Get(schema); !ok {
		client := NewRPCClient("tcp", schema, desc)
		pm.rpcclients.Set(schema, client)
		timer := time.NewTimer(Conf.RPCClientLookupInterval.Duration)
		pm.timers.Set(schema, timer)
		go func() {
			for {
				<-timer.C
				pm.DeletePushClient(schema)
			}
		}()
	}
	msg := fmt.Sprintf("add push client: %s, length: %d ", schema, pm.rpcclients.Length())
	SendAlert(schema, msg)
	log.Infof(msg)
}

func (pm *PusherManager) DeletePushClient(schema string) {
	pm.rpcclients.Delete(schema)
	if timer, ok := pm.timers.Get(schema); ok {
		timer.Stop()
	}
	pm.timers.Delete(schema)
	msg := fmt.Sprintf("delete push client %s, length: %d", schema, pm.rpcclients.Length())
	SendAlert(schema, msg)
	log.Infof(msg)
}

func (pm *PusherManager) UpdatePushClient(schema, desc string) {
	if timer, ok := pm.timers.Get(schema); ok {
		timer.Reset(Conf.RPCClientLookupInterval.Duration)
	}
	if _, ok := pm.rpcclients.Get(schema); !ok {
		pm.AddPushClient(schema, desc)
	}
}

func (pm *PusherManager) AddTask(task *model.Task) error {
	for client := range pm.rpcclients.Iter() {
		status, err := client.AddTask(task)
		if err != nil {
			log.Errorf("pusher manager add task status: %s, error: %s", status, err.Error())
			return err
		}
	}
	return nil
}

func (pm *PusherManager) DeleteTask(task *model.Task) error {
	for client := range pm.rpcclients.Iter() {
		status, err := client.DeleteTask(task)
		if err != nil {
			log.Errorf("pusher manager delete task status: %s, error: %s", status, err.Error())
			return err
		}
	}
	return nil
}

func (pm *PusherManager) UpdateTask(task *model.Task) error {
	for client := range pm.rpcclients.Iter() {
		status, err := client.UpdateTask(task)
		if err != nil {
			log.Errorf("pusher manager update task status: %s, error: %s", status, err.Error())
			return err
		}
	}
	return nil
}

func (pm *PusherManager) StartTask(task *model.Task) error {
	for client := range pm.rpcclients.Iter() {
		status, err := client.StartTask(task)
		if err != nil {
			log.Errorf("pusher manager start task status: %s, error: %s", status, err.Error())
			return err
		}
	}
	return nil
}

func (pm *PusherManager) StopTask(task *model.Task) error {
	for client := range pm.rpcclients.Iter() {
		status, err := client.StopTask(task)
		if err != nil {
			log.Errorf("pusher manager stop task status: %s, error: %s", status, err.Error())
			return err
		}
	}
	return nil
}
