package main

import (
	"mysql_byroad/model"

	log "github.com/Sirupsen/logrus"
)

type TaskManager struct {
	notifyTaskMap *NotifyTaskMap
	taskIdMap     *TaskIdMap
	rpcSchema     string
}

func NewTaskManager(rpcSchema string) *TaskManager {
	tm := &TaskManager{
		rpcSchema: rpcSchema,
	}
	return tm
}

/*
通过rpc，从monitor获取所有的任务信息
*/
func (tm *TaskManager) InitTasks() ([]*model.Task, error) {
	rpcClient := NewRPCClient(tm.rpcSchema)
	tasks, err := rpcClient.GetAllTasks("")
	if err != nil {
		return tasks, err
	}
	tm.taskIdMap = NewTaskIdMap(100)
	for _, t := range tasks {
		tm.taskIdMap.Set(t.ID, t)
	}
	tm.notifyTaskMap = NewNotifyTaskMap(tm.taskIdMap)
	log.Debug("notify task map: ", tm.notifyTaskMap)
	return tasks, nil
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
