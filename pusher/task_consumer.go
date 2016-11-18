package main

import (
	"mysql_byroad/model"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/nsqio/go-nsq"
)

/*
任务对应的nsq consumer
*/
type TaskConsumer struct {
	task     *model.Task
	consumer *nsq.Consumer
	config   *nsq.Config
}

func NewTaskConsumer(task *model.Task) (*TaskConsumer, error) {
	config := nsq.NewConfig()
	config.MaxAttempts = uint16(task.RetryCount + 1)
	config.MaxInFlight = task.RoutineCount
	config.DefaultRequeueDelay = time.Millisecond * time.Duration(task.ReSendTime)
	c, err := nsq.NewConsumer(genTaskTopic(task), genTaskChannel(task), config)
	if err != nil {
		log.Error("nsq new comsumer: ", err.Error())
		return nil, err
	}
	tc := &TaskConsumer{
		task:     task,
		consumer: c,
		config:   config,
	}
	return tc, nil
}

func (this *TaskConsumer) GetTopic() string {
	return this.task.Name
}

func (this *TaskConsumer) AddHandler(handler nsq.Handler) {
	this.consumer.AddConcurrentHandlers(handler, Conf.NSQConf.MaxConcurrentHandler)
}

func (this *TaskConsumer) StartConsume() error {
	return this.consumer.ConnectToNSQLookupds(Conf.NSQConf.LookupdHttpAddrs)
}

func (this *TaskConsumer) StopConsume() {
	this.consumer.Stop()
	<-this.consumer.StopChan
}

func (this *TaskConsumer) PauseConsume() {
	this.consumer.ChangeMaxInFlight(0)
}

func (this *TaskConsumer) UnPauseConsume() {
	this.consumer.ChangeMaxInFlight(this.task.RoutineCount)
}

/*
改变消费并发到指定的值
*/
func (this *TaskConsumer) ChangeConsume(task *model.Task) {
	this.task = task
	this.consumer.ChangeMaxInFlight(task.RoutineCount)
}

func (this *TaskConsumer) DisconnectFromNSQLookupds() error {
	for _, nsqlookupd := range Conf.NSQConf.LookupdHttpAddrs {
		err := this.consumer.DisconnectFromNSQLookupd(nsqlookupd)
		if err != nil {
			log.Errorf("task consumer %s disconnect from nsqlookupd %s: %s", this.task.Name, nsqlookupd, err.Error())
		}
	}
	return nil
}

func genTaskTopic(task *model.Task) string {
	return task.Name + "___kafka"
}

func genTaskChannel(task *model.Task) string {
	return task.Name + "___kafka"
}
