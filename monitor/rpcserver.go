package main

import (
	"mysql_byroad/model"
	"net"
	"net/http"
	"net/rpc"
	"sort"
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
	//this.register(configer.GetString("rpc", "schema"))
}

func (m *Monitor) GetAllTasks(username string, tasks *[]*model.Task) error {
	task := model.Task{
		ID:     2,
		Name:   "test",
		Apiurl: "http://localhost:8091/get",
		Stat:   "正常",
	}
	fields := make([]*model.NotifyField, 0, 10)
	field := model.NotifyField{
		Schema: "byroad",
		Table:  "task",
		Column: "name",
	}
	fields = append(fields, &field)
	task.Fields = fields
	ts := make([]*model.Task, 0, 10)
	ts = append(ts, &task)
	*tasks = ts
	sort.Sort(TaskSlice(*tasks))
	//queueManager.TasksQueueLen(*tasks)
	return nil
}
