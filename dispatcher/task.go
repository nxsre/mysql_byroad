package main

import (
	"mysql_byroad/model"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

type TaskManager struct {
	notifyTaskMap *NotifyTaskMap
	taskIdMap     *TaskIdMap
}

func NewTaskManager(ctx context.Context) *TaskManager {
	tm := &TaskManager{}
	return tm
}

func (tm *TaskManager) initTasks(ctx context.Context) {
	rpcClient := ctx.Value("dispatcher").(*Dispatcher).rpcClient
	conf := ctx.Value("dispatcher").(*Dispatcher).Config
	tasks, err := rpcClient.GetTasks(conf.DBInstanceName)
	if err != nil {
		log.Error("get all tasks: ", err.Error())
	}
	tm.taskIdMap = NewTaskIdMap(100)
	for _, t := range tasks {
		tm.taskIdMap.Set(t.ID, t)
	}
	for _, task := range tasks {
		log.Debug("task: ", task)
	}
	tm.notifyTaskMap = NewNotifyTaskMap(tm.taskIdMap)
	log.Debug("notify task map: ", tm.notifyTaskMap)
	tm.initTaskConsumers(ctx, tasks)
}

func (tm *TaskManager) initTaskConsumers(ctx context.Context, tasks []*model.Task) {
	consumers := make(map[string]*KafkaConsumer)
	config := ctx.Value("dispatcher").(*Dispatcher).Config
	kafkaHandler := NewKafkaEventHandler(ctx)
	for _, task := range tasks {
		for _, field := range task.Fields {
			topic := GenTopicName(field.Schema, field.Table)
			if _, ok := consumers[topic]; !ok {
				consumer, err := NewKafkaConsumer(field.Schema, field.Table, config.ZookeeperConf.Addrs)
				if err != nil {
					log.Errorf("new kafka consumer error: %s", err.Error())
					continue
				}
				consumer.HandleMessage()
				consumer.AddHandler(kafkaHandler)
				consumers[topic] = consumer
			}
		}
	}
	disp := ctx.Value("dispatcher").(*Dispatcher)
	disp.consumers = consumers
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
