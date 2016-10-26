package main

import (
	"mysql_byroad/model"
	"net"
	"net/http"
	"net/rpc"
	"runtime"
	"time"

	log "github.com/Sirupsen/logrus"
)

type RPCServer struct {
	protocol             string
	schema               string
	desc                 string
	listener             net.Listener
	taskManager          *TaskManager
	binlogStatistics     *model.BinlogStatistics
	kafkaConsumerManager *KafkaConsumerManager
	startTime            time.Time
}

func NewRPCServer(schema, desc string) *RPCServer {
	server := RPCServer{
		protocol:  "tcp",
		schema:    schema,
		desc:      desc,
		startTime: time.Now(),
	}
	return &server
}

func (this *RPCServer) initServer(taskManager *TaskManager, binlogStatistics *model.BinlogStatistics, kafkaConsumerManager *KafkaConsumerManager) {
	this.taskManager = taskManager
	this.binlogStatistics = binlogStatistics
	this.kafkaConsumerManager = kafkaConsumerManager
}

func (this *RPCServer) getSchema() string {
	return this.listener.Addr().String()
}

func (this *RPCServer) startRpcServer() {
	rpc.Register(this)
	rpc.HandleHTTP()
	l, e := net.Listen(this.protocol, this.schema)
	if e != nil {
		panic(e.Error())
	}
	go http.Serve(l, nil)
	this.listener = l
	log.Infof("start rpc server at %s", this.getSchema())
}

func (rs *RPCServer) AddTask(task *model.Task, status *string) error {
	log.Infof("rpc add task: %+v", task)
	*status = "sucess"
	rs.taskManager.taskIdMap.Set(task.ID, task)
	if task.Stat == model.TASK_STATE_START {
		rs.taskManager.notifyTaskMap.AddTask(task)
	}
	rs.kafkaConsumerManager.AddTask(task)
	return nil
}

func (rs *RPCServer) DeleteTask(task *model.Task, status *string) error {
	id := task.ID
	log.Info("rpc delete task: ", id)
	*status = "success"
	rs.taskManager.taskIdMap.Delete(id)
	rs.taskManager.notifyTaskMap.UpdateNotifyTaskMap(rs.taskManager.taskIdMap)
	rs.kafkaConsumerManager.DeleteTask(task)
	return nil
}

func (rs *RPCServer) UpdateTask(task *model.Task, status *string) error {
	log.Infof("rpc update task: %+v", task)
	*status = "success"
	rs.taskManager.taskIdMap.Set(task.ID, task)
	rs.taskManager.notifyTaskMap.UpdateNotifyTaskMap(rs.taskManager.taskIdMap)
	rs.kafkaConsumerManager.UpdateTask(task)
	return nil
}

func (rs *RPCServer) StartTask(task *model.Task, status *string) error {
	log.Infof("rpc start task: %+v", task)
	*status = "success"
	rs.taskManager.taskIdMap.Set(task.ID, task)
	rs.taskManager.notifyTaskMap.UpdateNotifyTaskMap(rs.taskManager.taskIdMap)
	rs.kafkaConsumerManager.StartTask(task)
	return nil
}

func (rs *RPCServer) StopTask(task *model.Task, status *string) error {
	log.Infof("rpc stop task: %+v", task)
	*status = "success"
	rs.taskManager.taskIdMap.Set(task.ID, task)
	rs.taskManager.notifyTaskMap.UpdateNotifyTaskMap(rs.taskManager.taskIdMap)
	rs.kafkaConsumerManager.StopTask(task)
	return nil
}

func (rs *RPCServer) GetBinlogStatistics(username string, statics *[]*model.BinlogStatistic) error {
	log.Info("rpc get binlog statistics")
	*statics = rs.binlogStatistics.Statistics
	return nil
}

func (rs *RPCServer) GetStatus(username string, st *map[string]interface{}) error {
	log.Info("rpc get status")
	start := rs.startTime
	duration := time.Now().Sub(start)
	statusMap := make(map[string]interface{})
	statusMap["Start"] = start.String()
	statusMap["Duration"] = duration.String()
	statusMap["routineNumber"] = runtime.NumGoroutine()
	*st = statusMap
	return nil
}
