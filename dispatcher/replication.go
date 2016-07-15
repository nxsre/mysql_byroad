package main

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type ReplicationClient struct {
	ServerId       uint32
	Host           string
	Port           uint16
	Username       string
	Password       string
	BinlogFilename string
	BinlogPosition uint32
	handler        []EventHandler
	running        bool
	StopChan       chan bool
}

type EventHandler interface {
	HandleEvent(evt *replication.BinlogEvent)
}

func (rep *ReplicationClient) Start() {
	rep.running = true
	go startReplication(rep)
}

func (rep *ReplicationClient) AddHandler(handler EventHandler) {
	rep.handler = append(rep.handler, handler)
}

func startReplication(rep *ReplicationClient) {
	defer func() {
		if err := recover(); err != nil {
			rep.StopChan <- true
		}
	}()
	syncer := replication.NewBinlogSyncer(rep.ServerId, "mysql")
	err := syncer.RegisterSlave(rep.Host, rep.Port, rep.Username, rep.Password)
	if err != nil {
		log.Panicf("start replication on %s:%d %s", rep.Host, rep.Port, err.Error())
	}
	filename := rep.BinlogFilename
	pos := rep.BinlogPosition
	log.Debugf("config filename %s, pos %d", filename, pos)
	if filename == "" || pos == 0 {
		binfo, _ := GetBinlogInfo()
		filename = binfo.Filename
		pos = binfo.Position
		log.Debugf("config db filename %s, pos %d", filename, pos)
		if filename == "" || pos == 0 {
			addr := fmt.Sprintf("%s:%d", rep.Host, rep.Port)
			c, err := client.Connect(addr, rep.Username, rep.Password, "")
			if err != nil {
				log.Panicf("start replication on %s:%d %s", rep.Host, rep.Port, err.Error())
			}
			rr, err := c.Execute("SHOW MASTER STATUS")
			if err != nil {
				log.Panicf("start replication on %s:%d %s", rep.Host, rep.Port, err.Error())
			}
			filename, _ = rr.GetString(0, 0)
			position, _ := rr.GetInt(0, 1)
			pos = uint32(position)
			c.Close()
		}
	}
	streamer, err := syncer.StartSync(mysql.Position{filename, pos})
	if err != nil {
		log.Panicf("start replication on %s:%d %s", rep.Host, rep.Port, err.Error())
	}
	log.Infof("start replication client on %s:%d at %s, %d", rep.Host, rep.Port, filename, pos)
	timeout := time.Second
	for rep.running {
		ev, err := streamer.GetEventTimeout(timeout)
		if err != nil {
			if err == replication.ErrGetEventTimeout {
				continue
			} else {
				log.Error(err.Error())
				continue
			}
		}
		for _, handler := range rep.handler {
			handler.HandleEvent(ev)
		}
	}
	rep.StopChan <- true
	log.Infof("stop replication client on %s:%d", rep.Host, rep.Port)
}

func (rep *ReplicationClient) Stop() {
	rep.running = false
}
