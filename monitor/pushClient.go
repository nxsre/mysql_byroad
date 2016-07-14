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
	log.Infof("add push client: %s, length: %d ", schema, len(pm.rpcclients))
}

func (pm *PusherManager) DeletePushClient(schema string) {
	var index int
	for idx, client := range pm.rpcclients {
		if client.Schema == schema {
			index = idx
			pm.rpcclients = append(pm.rpcclients[:index], pm.rpcclients[index+1:]...)
			break
		}
	}
	log.Infof("delete push client %s, length: %d", schema, len(pm.rpcclients))
}

func (pm *PusherManager) AddTask(task *model.Task) {
	for _, client := range pm.rpcclients {
		status, err := client.AddTask(task)
		if err != nil {
			log.Errorf("pusher manager add task status: %s, error: %s", status, err.Error())
		}
	}
}
