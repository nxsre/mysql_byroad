package main

import (
	"mysql_byroad/model"
	"mysql_byroad/nsq"
	"sync"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

type Enqueuer interface {
	Enqueue(name string, evt interface{})
}
type EventEnqueuer struct {
	queueManager Enqueuer
	taskManager *TaskManager
	sync.WaitGroup
}

func NewEventEnqueuer(ctx context.Context) *EventEnqueuer {
	disp := ctx.Value("dispatcher").(*Dispatcher)
	config := disp.Config
	ee := &EventEnqueuer{}
	ee.taskManager = disp.taskManager
	qm, err := nsqm.NewNSQManager(config.NSQConf.LookupdHttpAddrs, config.NSQConf.NsqdAddrs, nil)
	if err != nil {
		log.Error(err.Error())
	}
	qm.InitProducers()
	qm.ProducerUpdateLoop()
	ee.queueManager = qm
	return ee
}

/*
根据任务数量并发的将消息写入消息队列中
*/
func (this *RowsEventHandler) Enqueue(schema, table, event string, taskFieldMap map[int64][]*model.ColumnValue) {
	log.Debug(taskFieldMap)
	for taskid, fields := range taskFieldMap {
		log.Debug("rows event handler enqueue, task id ", taskid)
		this.eventEnqueuer.Add(1)
		go this.enqueue(schema, table, event, taskid, fields)
	}
	this.eventEnqueuer.Wait()
}

/*
根据字段的内容，对订阅了的任务组装相应的消息内容，对于update操作，判断内容是否有变化，
没有变化的字段将不会添加到消息体中
*/
func (this *RowsEventHandler) enqueue(schema, table, event string, taskid int64, fields []*model.ColumnValue) {
	ntyevt := new(model.NotifyEvent)
	ntyevt.Keys = make([]string, 0)
	ntyevt.Fields = make([]*model.ColumnValue, 0)
	task := this.taskManager.GetTask(taskid)
	if task == nil {
		this.eventEnqueuer.Done()
		return
	}
	updateChanged := false
	for _, f := range fields {
		tf := this.taskManager.GetTaskField(task, schema, table, f.ColunmName)
		if tf == nil {
			continue
		}
		if event != model.UPDATE_EVENT {
			if tf.Send == 1 {
				ntyevt.Fields = append(ntyevt.Fields, f)
			} else {
				ntyevt.Keys = append(ntyevt.Keys, f.ColunmName)
			}
		} else {
			if tf.Send == 1 {
				ntyevt.Fields = append(ntyevt.Fields, f)
				if !isEqual(f.Value, f.OldValue) {
					updateChanged = true
				}
			} else if !isEqual(f.Value, f.OldValue) {
				ntyevt.Keys = append(ntyevt.Keys, f.ColunmName)
				updateChanged = true
			}
		}
	}
	if len(ntyevt.Fields) == 0 && len(ntyevt.Keys) == 0 {
		this.eventEnqueuer.Done()
		return
	} else if event == model.UPDATE_EVENT && !updateChanged {
		this.eventEnqueuer.Done()
		return
	}
	ntyevt.Schema = schema
	ntyevt.Table = table
	ntyevt.Event = event
	ntyevt.TaskID = task.ID
	name := genTaskQueueName(task)
	this.eventEnqueuer.queueManager.Enqueue(name, ntyevt)
	this.eventEnqueuer.Done()
}
