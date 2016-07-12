package main

import (
	"mysql_byroad/model"
	"net"
	"net/http"
	"net/rpc"

	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
)

type Monitor struct {
	protocol string
	schema   string
	desc     string
}

func NewRPCServer(protocol, schema, desc string) *Monitor {
	monitor := Monitor{
		protocol: protocol,
		schema:   schema,
		desc:     desc,
	}
	return &monitor
}

func (this *Monitor) start() {
	rpc.Register(this)
	rpc.HandleHTTP()
	l, e := net.Listen(this.protocol, this.schema)
	if e != nil {
		panic(e.Error())
	}
	go http.Serve(l, nil)
}

func (m *Monitor) GetAllTasks(username string, tasks *[]*model.Task) error {
	ts, err := model.GetAllTask()
	if err != nil {
		return err
	}
	*tasks = ts
	log.Debugf("tasks :%+v", ts)
	return nil
}
