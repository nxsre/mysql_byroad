package main

import (
	log "github.com/Sirupsen/logrus"
)

func main() {
	config := InitConfig()
	InitLog(config.Logfile, config.LogLevel)
	InitAlert(config.AlertConfig)
	log.Debugf("Conf: %+v", config)
	dispatcher := NewDispatcher(config)
	dispatcher.Start()
	dispatcher.HandleSignal()
}
