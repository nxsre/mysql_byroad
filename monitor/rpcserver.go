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
type ServiceSignal struct {
	Code   string
	Schema string
	Desc   string
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
	log.Infof("start rpc server at %s", this.schema)
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

func (m *Monitor) GetTaskByInstanceName(dbname string, tasks *[]*model.Task) error {
	ts, err := model.GetTaskByInstanceName(dbname)
	if err != nil {
		return err
	}
	*tasks = ts
	return nil
}

func (m *Monitor) HandlePushClientSignal(ss *ServiceSignal, status *string) error {
	log.Debugf("push client signal %+v", ss)
	if ss.Code == "1" {
		pusherManager.AddPushClient(ss.Schema, ss.Desc)
	} else if ss.Code == "0" {
		pusherManager.DeletePushClient(ss.Schema)
	} else if ss.Code == "2" {
		pusherManager.UpdatePushClient(ss.Schema, ss.Desc)
	}
	*status = "OK"
	return nil
}

func (m *Monitor) HandleDispatchClientSignal(ss *ServiceSignal, status *string) error {
	log.Debugf("dispatch client signal %+v", ss)
	if ss.Code == "1" {
		dispatcherManager.AddDispatchClient(ss.Schema, ss.Desc)
	} else if ss.Code == "0" {
		dispatcherManager.DeleteDispatchClient(ss.Schema)
	} else if ss.Code == "2" {
		dispatcherManager.UpdateDispatchClient(ss.Schema, ss.Desc)
	}
	*status = "OK"
	return nil
}
