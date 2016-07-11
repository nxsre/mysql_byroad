package main

import (
	"mysql_byroad/model"

	log "github.com/Sirupsen/logrus"
)

type TaskManager struct {
	rpcClient     *RPCClient
	rpcServer     *RPCServer
	notifyTaskMap *NotifyTaskMap
	taskIdMap     *TaskIdMap
}

func NewTaskManager(rpcClientSchema, rpcServerSchema string) *TaskManager {
	tm := &TaskManager{}
	tm.rpcClient = NewRPCClient("tcp", rpcClientSchema, "")
	tm.rpcServer = NewRPCServer("tcp", rpcServerSchema, "")
	tm.rpcServer.startRpcServer()
	tm.initTasks()
	return tm
}

func (tm *TaskManager) initTasks() {
	tasks, err := tm.rpcClient.GetAllTasks("")
	if err != nil {
		log.Error(err.Error())
	}
	tm.taskIdMap = NewTaskIdMap(100)
	for _, t := range tasks {
		tm.taskIdMap.Set(t.ID, t)
	}
	log.Debug("task map:", tm.taskIdMap)
	tm.notifyTaskMap = NewNotifyTaskMap(tm.taskIdMap)
	log.Debug("notify task map: ", tm.notifyTaskMap)
}

func (tm *TaskManager) InNotifyTable(schema, table string) bool {
	log.Debug("in notify table:", tm.notifyTaskMap.InNotifyTable(schema, table))
	return tm.notifyTaskMap.InNotifyTable(schema, table)
}

func (tm *TaskManager) InNotifyField(schema, table, column string) bool {
	log.Debug("in notify field:", tm.notifyTaskMap.InNotifyField(schema, table, column))
	return tm.notifyTaskMap.InNotifyField(schema, table, column)
}

func (tm *TaskManager) GetNotifyTaskIDs(schema, table, column string) []int64 {
	return tm.notifyTaskMap.GetNotifyTaskIDs(schema, table, column)
}

func (tm *TaskManager) GetTaskField(task *model.Task, schema, table, column string) *model.NotifyField {
	for _, field := range task.Fields {
		if isSchemaMatch(field.Schema, schema) && isTableMatch(field.Table, table) && field.Column == column {
			return field
		}
	}
	return nil
}

func (tm *TaskManager) GetTask(id int64) *model.Task {
	return tm.taskIdMap.Get(id)
}
