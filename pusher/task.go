package main

import (
	"mysql_byroad/model"

	"github.com/nsqio/go-nsq"
)

type TaskManager struct {
	taskMap         *TaskIdMap
	taskConsumerMap map[int64]*nsq.Consumer
}

func NewTaskManager() *TaskManager {
	tm := &TaskManager{
		taskMap:         NewTaskIdMap(100),
		taskConsumerMap: make(map[int64]*nsq.Consumer, 100),
	}

	return tm
}

func (tm *TaskManager) InitTaskMap(tasks []*model.Task) {
	for _, task := range tasks {
		tm.taskMap.Set(task.ID, task)
	}
}

func (tm *TaskManager) InitTasKRoutine() {
	for _, task := range tm.taskMap.cmap {
		c := NewNSQConsumer(task.Name, task.Name, task.RoutineCount)
		tm.taskConsumerMap[task.ID] = c
	}
}

func (tm *TaskManager) GetTask(id int64) *model.Task {
	return tm.taskMap.Get(id)
}

func (tm *TaskManager) AddTask(task *model.Task) {
	tm.taskMap.Set(task.ID, task)
}

func (tm *TaskManager) StopTask(task *model.Task) {
	if c, ok := tm.taskConsumerMap[task.ID]; ok {
		c.Stop()
	}
}

func (tm *TaskManager) StartTask(task *model.Task) {
	tm.taskMap.Set(task.ID, task)
	c := NewNSQConsumer(task.Name, task.Name, task.RoutineCount)
	tm.taskConsumerMap[task.ID] = c
}

func (tm *TaskManager) DeleteTask(task *model.Task) {
	tm.taskMap.Delete(task.ID)
	if c, ok := tm.taskConsumerMap[task.ID]; ok {
		c.Stop()
	}
}
