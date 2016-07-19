package main

import (
	"mysql_byroad/model"

	"github.com/nsqio/go-nsq"
)

type TaskManager struct {
	taskMap         *TaskIdMap
	taskConsumerMap map[int64][]*nsq.Consumer
}

func NewTaskManager() *TaskManager {
	tm := &TaskManager{
		taskMap:         NewTaskIdMap(100),
		taskConsumerMap: make(map[int64][]*nsq.Consumer, 100),
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
		consumers := tm.newConsumers(task)
		tm.taskConsumerMap[task.ID] = consumers
	}
}

func (tm *TaskManager) GetTask(id int64) *model.Task {
	return tm.taskMap.Get(id)
}

func (tm *TaskManager) AddTask(task *model.Task) {
	tm.taskMap.Set(task.ID, task)
}

func (tm *TaskManager) StopTask(task *model.Task) {
	tm.stopConsumers(task)
}

func (tm *TaskManager) StartTask(task *model.Task) {
	tm.taskMap.Set(task.ID, task)
	consumers := tm.newConsumers(task)
	tm.taskConsumerMap[task.ID] = consumers
}

func (tm *TaskManager) DeleteTask(task *model.Task) {
	tm.taskMap.Delete(task.ID)
	tm.stopConsumers(task)
}

func (tm *TaskManager) newConsumers(task *model.Task) []*nsq.Consumer {
	consumers := make([]*nsq.Consumer, 0, 10)
	for i := 0; i < task.RoutineCount; i++ {
		c := NewNSQConsumer(task.Name, task.Name, 1)
		consumers = append(consumers, c)
	}
	return consumers
}

func (tm *TaskManager) stopConsumers(task *model.Task) {
	if cs, ok := tm.taskConsumerMap[task.ID]; ok {
		for _, c := range cs {
			c.Stop()
		}
	}
}
