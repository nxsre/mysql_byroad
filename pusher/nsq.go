package main

import (
	"encoding/json"
	"mysql_byroad/model"

	log "github.com/Sirupsen/logrus"
	"github.com/nsqio/go-nsq"
)

type MessageHandler struct {
	log *LogFile
}

func NewMessageHandler() *MessageHandler {
	logfile, err := NewLogFile(Conf.LogConfig.LogPath)
	if err != nil {
		log.Errorf("new log file error: %s", err.Error())
	}
	return &MessageHandler{
		log: logfile,
	}
}

func (h *MessageHandler) HandleMessage(msg *nsq.Message) error {
	log.Debug(string(msg.Body))
	evt := new(model.NotifyEvent)
	err := json.Unmarshal(msg.Body, evt)
	evt.RetryCount = int(msg.Attempts) - 1
	ret, err := sendClient.SendMessage(evt)
	log.Debugf("send message ret %s, error: %v", ret, err)
	if h.log != nil {
		var errmsg string
		if err != nil {
			errmsg = err.Error()
		}
		err := h.log.WritePayload(ret, errmsg, evt)
		if err != nil {
			log.Errorf("log message error: %s", err.Error())
		}
	}
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
