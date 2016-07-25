package main

import (
	"os"

	log "github.com/Sirupsen/logrus"
)

func init() {
	log.SetOutput(os.Stderr)
	log.SetLevel(log.InfoLevel)
}

func InitLog() {
	file, err := os.OpenFile(Conf.Logfile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Errorf("init log file error: %s", err.Error())
		return
	}
	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(file)
}
