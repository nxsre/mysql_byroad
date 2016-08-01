package main

import (
	"mysql_byroad/model"

	log "github.com/Sirupsen/logrus"
	"github.com/nsqio/go-nsq"
)

type TaskManager struct {
	taskMap         *TaskIdMap
	taskConsumerMap map[int64][]*nsq.Consumer
}

func NewTaskManager() *TaskManager {
	tm := &TaskManager{
		taskMap:         NewTaskIdMap(100),
		taskConsumerMap: make(map[int64][]*nsq.Consumer, 10),
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
		if task.Stat == model.TASK_STATE_START {
			consumers := tm.newConsumers(task)
			tm.taskConsumerMap[task.ID] = consumers
		}
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

/*
新建consumer，因此在使用应保证之前的consumer都已停止，要不然之前创建的consumer可能会继续消费
*/
func (tm *TaskManager) StartTask(task *model.Task) {
	tm.taskMap.Set(task.ID, task)
	consumers := tm.newConsumers(task)
	tm.taskConsumerMap[task.ID] = consumers
}

func (tm *TaskManager) DeleteTask(task *model.Task) {
	tm.taskMap.Delete(task.ID)
	tm.stopConsumers(task)
	delete(tm.taskConsumerMap, task.ID)
}

/*
根据任务的状态更新任务，如果任务的状态是停止或者任务的状态是正常，但是并发数没有改变，则不对consumer做操作，
如果状态为正常，并且并发数有改变，则相应的增加或减少consumer
*/
func (tm *TaskManager) UpdateTask(task *model.Task) {
	oldTask := tm.GetTask(task.ID)
	tm.taskMap.Set(task.ID, task)
	if task.Stat == model.TASK_STATE_STOP {
		return
	}
	if oldTask == nil {
		return
	}
	if oldTask.RoutineCount == task.RoutineCount {
		return
	}
	if oldTask.RoutineCount < task.RoutineCount {
		tm.incConsumers(task, task.RoutineCount-oldTask.RoutineCount)
	} else {
		tm.descConsumers(task, oldTask.RoutineCount-task.RoutineCount)
	}
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

func (tm *TaskManager) incConsumers(task *model.Task, num int) {
	log.Debugf("inc consumers %+v,  %d", task, num)
	if consums, ok := tm.taskConsumerMap[task.ID]; ok {
		log.Debugf("before: len consumers %d", len(consums))
		for i := 0; i < num; i++ {
			c := NewNSQConsumer(task.Name, task.Name, 1)
			consums = append(consums, c)
		}
		tm.taskConsumerMap[task.ID] = consums
		log.Debugf("after: len consumers %d", len(consums))
	}
}

func (tm *TaskManager) descConsumers(task *model.Task, num int) {
	log.Debugf("desc consumer %+v, %d", task, num)
	if consums, ok := tm.taskConsumerMap[task.ID]; ok {
		log.Debugf("before: len consumers %d", len(consums))
		if len(consums) <= num {
			tm.stopConsumers(task)
		} else {
			for i := 0; i < num; i++ {
				c := consums[i]
				c.Stop()
				consums[i] = nil
			}
			consums = consums[num:]
		}
		tm.taskConsumerMap[task.ID] = consums
		log.Debugf("after: len consumers %d", len(consums))
	}
}
