package main

import (
	"encoding/json"
	"mysql_byroad/model"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/nsqio/go-nsq"
)

type MessageHandler struct {
}

func (h *MessageHandler) HandleMessage(msg *nsq.Message) error {
	log.Debug(string(msg.Body))
	evt := new(model.NotifyEvent)
	err := json.Unmarshal(msg.Body, evt)
	evt.RetryCount = int(msg.Attempts)
	ret, err := sendClient.SendMessage(evt)
	log.Debugf("send message ret %s, error: %v", ret, err)
	if !isSuccessSend(ret) {
		if err != nil {
			sendClient.LogSendError(evt, err.Error())
		} else {
			sendClient.LogSendError(evt, ret)
		}
		msg.RequeueWithoutBackoff(-1)
	}
	return nil
}

func NewNSQConsumer(topic, channel string, concurrency int) *nsq.Consumer {
	config := nsq.NewConfig()
	c, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		log.Error("nsq new comsumer: ", err.Error())
	}
	h := &MessageHandler{}
	c.AddConcurrentHandlers(h, concurrency)
	err = c.ConnectToNSQLookupds(Conf.NSQConf.LookupdHttpAddrs)
	if err != nil {
		log.Error("nsq connect to nsq lookupds: ", err.Error())
	}
	return c
}

func NewTaskConsumer(task *model.Task) *nsq.Consumer {
	config := nsq.NewConfig()
	config.MaxAttempts = uint16(task.RetryCount)
	config.DefaultRequeueDelay = time.Millisecond * time.Duration(task.ReSendTime)
	c, err := nsq.NewConsumer(task.Name, task.Name, config)
	if err != nil {
		log.Error("nsq new comsumer: ", err.Error())
	}
	h := &MessageHandler{}
	c.AddHandler(h)
	err = c.ConnectToNSQLookupds(Conf.NSQConf.LookupdHttpAddrs)
	if err != nil {
		log.Error("nsq connect to nsq lookupds: ", err.Error())
	}
	return c
}
