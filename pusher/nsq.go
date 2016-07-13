package main

import (
	"encoding/json"
	"mysql_byroad/model"

	log "github.com/Sirupsen/logrus"
	"github.com/nsqio/go-nsq"
)

type MessageHandler struct {
}

func (h *MessageHandler) HandleMessage(msg *nsq.Message) error {
	log.Debug(string(msg.Body))
	evt := new(model.NotifyEvent)
	err := json.Unmarshal(msg.Body, evt)
	ret, err := sendMessage(evt)
	log.Debugf("send message ret %s, error: %s", ret, err)
	return nil
}

func NewNSQConsumer(topic, channel string, concurrency int) *nsq.Consumer{
	log.Debugf("new consumer %s/%s", topic, channel)
	config := nsq.NewConfig()
	c, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		log.Error(err.Error())
	}
	h := &MessageHandler{}
	c.AddConcurrentHandlers(h, concurrency)
	err = c.ConnectToNSQLookupds(Conf.NSQConf.LookupdHttpAddrs)
	if err != nil {
		log.Error(err.Error())
	}
    return c
}
