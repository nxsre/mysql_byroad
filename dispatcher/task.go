package main

import (
	"fmt"
	"mysql_byroad/model"
	"sync"

	log "github.com/Sirupsen/logrus"
)

type TaskManager struct {
	notifyTaskMap *NotifyTaskMap
	taskIdMap     *TaskIdMap
}

var taskManager *TaskManager
var once sync.Once

func newTaskManager() *TaskManager {
	tm := &TaskManager{}
	tm.initTasks()
	return tm
}

func GetTaskManagerInstance() *TaskManager {
	once.Do(func() {
		taskManager = newTaskManager()
	})
	return taskManager
}

func (tm *TaskManager) initTasks() {
	conf := Conf.MonitorConf
	schema := fmt.Sprintf("%s:%d", conf.Host, conf.RpcPort)
	rpcClient := NewRPCClient("tcp", schema, "")
	tasks, err := rpcClient.GetAllTasks("")
	if err != nil {
		log.Error("get all tasks: ", err.Error())
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
	log.Debugf("%s, %s in notify table: %v", schema, table, tm.notifyTaskMap.InNotifyTable(schema, table))
	return tm.notifyTaskMap.InNotifyTable(schema, table)
}

func (tm *TaskManager) InNotifyField(schema, table, column string) bool {
	log.Debugf("%s, %s, %s in notify field: %v", schema, table, column, tm.notifyTaskMap.InNotifyField(schema, table, column))
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
