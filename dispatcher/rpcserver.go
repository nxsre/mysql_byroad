package main

import (
	"mysql_byroad/common"
	"mysql_byroad/model"
	"net"
	"net/http"
	"net/rpc"

	log "github.com/Sirupsen/logrus"
)

type RPCServer struct {
	protocol string
	schema   string
	desc     string
	tm       *TaskManager
}

func NewRPCServer(protocol, schema, desc string) *RPCServer {
	server := RPCServer{
		protocol: protocol,
		schema:   schema,
		desc:     desc,
	}
	return &server
}

func (this *RPCServer) startRpcServer() {
	rpc.Register(this)
	rpc.HandleHTTP()
	l, e := net.Listen(this.protocol, this.schema)
	if e != nil {
		panic(e.Error())
	}
	go http.Serve(l, nil)
}

func (rs *RPCServer) AddTask(task *model.Task, status *string) error {
	log.Debug("add task: ", task)
	*status = "sucess"
	rs.tm.taskIdMap.Set(task.ID, task)
	if task.Stat == common.TASK_STATE_START {
		rs.tm.notifyTaskMap.AddTask(task)
	}
	return nil
}

func (rs *RPCServer) DeleteTask(id int64, status *string) error {
	log.Debug("delete task: ", id)
	*status = "success"
	rs.tm.taskIdMap.Delete(id)
	rs.tm.notifyTaskMap.UpdateNotifyTaskMap(rs.tm.taskIdMap)
	return nil
}

func (rs *RPCServer) UpdateTask(task *model.Task, status *string) error {
	log.Debug("update task:", task)
	*status = "success"
	rs.tm.taskIdMap.Set(task.ID, task)
	rs.tm.notifyTaskMap.UpdateNotifyTaskMap(rs.tm.taskIdMap)
	return nil
}

func (rs *RPCServer) GetDBMap(dbname string, os *model.OrderedSchemas) error {
	log.Debug("get db map:", dbname)
	var cm columnMap = make(map[string]map[string][]string, 1)
	cm[dbname] = DBMap[dbname]
	*os = getOrderedColumnsList(cm)
	return nil
}

func (rs *RPCServer) GetAllDBMap(dbname string, os *model.OrderedSchemas) error {
	log.Debug("get all db map")
	*os = getOrderedColumnsList(DBMap)
	return nil
}
