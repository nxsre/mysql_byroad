package main

import (
	"os"

	log "github.com/Sirupsen/logrus"
)

func init() {
	log.SetOutput(os.Stderr)
	log.SetLevel(log.DebugLevel)
}
