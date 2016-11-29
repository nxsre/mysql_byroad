package main

import (
	"mysql_byroad/model"

	"fmt"

	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/nsqio/go-nsq"
	"github.com/streamrail/concurrent-map"
)

type TaskConsumerManager struct {
	taskConsumers cmap.ConcurrentMap
	handler       nsq.Handler
	taskIdMap     *TaskIdMap
}

func NewTaskConsumerManager() *TaskConsumerManager {
	tcm := &TaskConsumerManager{
		taskConsumers: cmap.New(),
		taskIdMap:     NewTaskIdMap(10),
		handler:       NewMessageHandler(),
	}
	return tcm
}

/*
为所有的任务生成消息推送的消费者，如果任务的push stat是1，则开始消费推送
*/
func (this *TaskConsumerManager) Init(tasks []*model.Task) {
	for _, task := range tasks {
		taskConsumer, err := this.AddTask(task)
		if err != nil {
			log.Errorf("task consumer manager init: %s", err.Error())
			continue
		}
		if task.Stat == model.TASK_STATE_START || task.PushStat == model.TASK_STAT_PUSH {
			err := taskConsumer.StartConsume()
			if err != nil {
				log.Errorf("task consumer manager start %s: %s", task.Name, err.Error())
				continue
			}
		}
	}
}

/*
添加一个任务,返回生成的TaskConsumer
*/
func (this *TaskConsumerManager) AddTask(task *model.Task) (*TaskConsumer, error) {
	taskConsumer, err := NewTaskConsumer(task)
	if err != nil {
		return nil, err
	}
	taskConsumer.AddHandler(this.handler)
	this.taskConsumers.Set(task.Name, taskConsumer)
	this.taskIdMap.Set(task.ID, task)
	return taskConsumer, nil
}

/*
开始任务，连接到nsqlookupd，开始消费
*/
func (this *TaskConsumerManager) StartTask(task *model.Task) error {
	obj, ok := this.taskConsumers.Get(task.Name)
	if !ok {
		return fmt.Errorf("task consumer %s not exists", task.Name)
	}
	taskConsumer := obj.(*TaskConsumer)
	this.taskIdMap.Set(task.ID, task)
	return taskConsumer.StartConsume()
}

/*
删除一个任务，停止消费，并断开与nsqlookupd的连接
*/
func (this *TaskConsumerManager) DeleteTask(task *model.Task) (*TaskConsumer, error) {
	obj, ok := this.taskConsumers.Get(task.Name)
	if !ok {
		return nil, fmt.Errorf("task consumer %s not exists", task.Name)
	}
	taskConsumer := obj.(*TaskConsumer)
	this.taskConsumers.Remove(task.Name)
	taskConsumer.StopConsume()
	this.taskIdMap.Delete(task.ID)
	return taskConsumer, nil
}

/*
修改任务的消费并发数
*/
func (this *TaskConsumerManager) ChangeTaskRoutineCount(task *model.Task) error {
	obj, ok := this.taskConsumers.Get(task.Name)
	if !ok {
		return fmt.Errorf("task consumer %s not exists", task.Name)
	}
	taskConsumer := obj.(*TaskConsumer)
	taskConsumer.ChangeConsume(task)
	this.taskIdMap.Set(task.ID, task)
	return nil
}

func (this *TaskConsumerManager) PauseTask(task *model.Task) error {
	obj, ok := this.taskConsumers.Get(task.Name)
	if !ok {
		return fmt.Errorf("task consumer %s not exists", task.Name)
	}
	taskConsumer := obj.(*TaskConsumer)
	taskConsumer.PauseConsume()
	this.taskIdMap.Set(task.ID, task)
	return nil
}

func (this *TaskConsumerManager) UnPauseTask(task *model.Task) error {
	obj, ok := this.taskConsumers.Get(task.Name)
	if !ok {
		return fmt.Errorf("task consumer %s not exists", task.Name)
	}
	taskConsumer := obj.(*TaskConsumer)
	taskConsumer.UnPauseConsume()
	this.taskIdMap.Set(task.ID, task)
	return nil
}

func (this *TaskConsumerManager) GetTask(taskid int64) *model.Task {
	return this.taskIdMap.Get(taskid)
}

/*
更新任务的推送并发数
*/
func (this *TaskConsumerManager) UpdateTask(task *model.Task) (*model.Task, error) {
	oldtask := this.taskIdMap.Get(task.ID)
	if oldtask == nil {
		return nil, fmt.Errorf("task consumer %s not exists", task.Name)
	}
	this.taskIdMap.Set(task.ID, task)
	err := this.ChangeTaskRoutineCount(task)
	return oldtask, err
}

func (this *TaskConsumerManager) StopTask(task *model.Task) error {
	obj, ok := this.taskConsumers.Get(task.Name)
	if !ok {
		return fmt.Errorf("task consumer %s not exists", task.Name)
	}
	taskConsumer := obj.(*TaskConsumer)
	taskConsumer.StopConsume()
	this.taskIdMap.Set(task.ID, task)
	return nil
}

func (this *TaskConsumerManager) StopAllTask() {
	var wg sync.WaitGroup
	for tuple := range this.taskConsumers.IterBuffered() {
		taskConsumer := tuple.Val.(*TaskConsumer)
		wg.Add(1)
		go func(consumer *TaskConsumer) {
			consumer.StopConsume()
			wg.Done()
		}(taskConsumer)
	}
	wg.Wait()
}
