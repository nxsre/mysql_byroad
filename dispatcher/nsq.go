package main

import (
	"encoding/json"

	log "github.com/Sirupsen/logrus"
	"github.com/nsqio/go-nsq"
)

type QueueManager struct {
	lookupdAddrs []string
	nodesInfo    []string
}

func NewQueueManager() {

}

func (qm *QueueManager) GetProducer() *nsq.Producer {
	config := nsq.NewConfig()
	p, _ := nsq.NewProducer("127.0.0.1:4150", config)
	return p
}

func (qm *QueueManager) Enqueue(name string, evt interface{}) {
	p := qm.GetProducer()
	evtMsg, _ := json.Marshal(evt)
	err := p.Publish(name, evtMsg)
	if err != nil {
		log.Error(err.Error())
	}
}
