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
	evt.RetryCount = int(msg.Attempts) - 1
	ret, err := sendClient.SendMessage(evt)
	log.Debugf("send message ret %s, error: %v", ret, err)
	if !isSuccessSend(ret) {
		var reason string
		if err != nil {
			reason = err.Error()
		} else {
			reason = ret
		}
		handleAlert(evt, reason)
		sendClient.LogSendError(evt, reason)
		msg.RequeueWithoutBackoff(-1)
	}
	return nil
}
