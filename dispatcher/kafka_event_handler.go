package main

import (
	"mysql_byroad/nsq"

	"github.com/Shopify/sarama"
	"github.com/siddontang/go/log"
	"golang.org/x/net/context"
)

type KafkaEventHandler struct {
	queueManager Enqueuer
	taskManager  *TaskManager
	dispatcher   *Dispatcher
}

func NewKafkaEventHandler(ctx context.Context) *KafkaEventHandler {
	disp := ctx.Value("dispatcher").(*Dispatcher)
	config := disp.Config
	keh := &KafkaEventHandler{}
	qm, err := nsqm.NewNSQManager(config.NSQConf.LookupdHttpAddrs, config.NSQConf.NsqdAddrs, nil)
	if err != nil {
		log.Error(err.Error())
	}
	qm.InitProducers()
	qm.ProducerUpdateLoop()
	keh.queueManager = qm
	keh.dispatcher = disp
	keh.taskManager = disp.taskManager
	return keh
}

func (keh *KafkaEventHandler) HandleKafkaEvent(evt *sarama.ConsumerMessage) {

}
