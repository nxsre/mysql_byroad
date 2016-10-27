package main

import "mysql_byroad/model"

type TaskManager struct {
	taskMap   *TaskMap
	rpcSchema string
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
	tm.taskMap = NewTaskMap(100)
	for _, t := range tasks {
		tm.taskMap.Set(t.Name, t)
	}
	return tasks, nil
}

func (tm *TaskManager) GetTaskField(task *model.Task, schema, table, column string) *model.NotifyField {
	for _, field := range task.Fields {
		if isSchemaMatch(field.Schema, schema) && isTableMatch(field.Table, table) && field.Column == column {
			return field
		}
	}
	return nil
}

func (tm *TaskManager) GetTask(name string) *model.Task {
	return tm.taskMap.Get(name)
}
