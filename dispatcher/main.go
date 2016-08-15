package main

import (
	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	config := InitConfig()
	InitLog(config.Logfile, config.LogLevel)
	log.Debugf("Conf: %+v", config)
	dispatcher := NewDispatcher(config)
	dispatcher.Start()
	dispatcher.HandleSignal()
}
