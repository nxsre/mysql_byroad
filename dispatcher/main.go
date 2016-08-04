package main

import (
	log "github.com/Sirupsen/logrus"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	config := InitConfig()
	InitLog(config.Logfile, config.LogLevel)
	log.Debugf("Conf: %+v", config)
	dispatcher := NewDispatcher(config)
	dispatcher.Start()
	dispatcher.HandleSignal()
}
