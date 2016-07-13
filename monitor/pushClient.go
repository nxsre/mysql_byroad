package main

import (
	"mysql_byroad/model"

	log "github.com/Sirupsen/logrus"
)

type PusherManager struct {
	rpcclients []*RPCClient
	schemas    []string
}

func NewPusherManager() *PusherManager {
	pm := &PusherManager{
		rpcclients: make([]*RPCClient, 0, 10),
		schemas:    make([]string, 0, 10),
	}
	return pm
}

func (pm *PusherManager) AddPushClient(schema string) {
	client := NewRPCClient("tcp", schema, "")
	pm.rpcclients = append(pm.rpcclients, client)
}

func (pm *PusherManager) DeletePushClient(schema string) {
	var index int
	for idx, client := range pm.rpcclients {
		if client.Schema == schema {
			index = idx
			break
		}
	}
	pm.rpcclients = append(pm.rpcclients[0:index], pm.rpcclients[index+1:len(pm.rpcclients)]...)
}

func (pm *PusherManager) AddTask(task *model.Task) {
	for _, client := range pm.rpcclients {
		status, err := client.AddTask(task)
		log.Errorf("pusher manager add task status: %s, error: %s", status, err.Error())
	}
}
