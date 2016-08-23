package main

import (
	"mysql_byroad/model"
	"mysql_byroad/nsq"
	"sync"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

type KafkaEventHandler struct {
	queueManager Enqueuer
	taskManager  *TaskManager
	dispatcher   *Dispatcher
}

func NewKafkaEventHandler(ctx context.Context) *KafkaEventHandler {
	disp := ctx.Value("dispatcher").(*Dispatcher)
	config := disp.Config
	keh := &KafkaEventHandler{}
	qm, err := nsqm.NewNSQManager(config.NSQConf.LookupdHttpAddrs, config.NSQConf.NsqdAddrs, nil)
	if err != nil {
		log.Error(err.Error())
	}
	qm.InitProducers()
	qm.ProducerUpdateLoop()
	keh.queueManager = qm
	keh.dispatcher = disp
	keh.taskManager = disp.taskManager
	return keh
}

func (keh *KafkaEventHandler) HandleKafkaEvent(evt *Entity) {
	switch evt.EventType {
	case "INSERT":
		keh.HandleInsertEvent(evt)
	case "DELETE":
		keh.HandleDeleteEvent(evt)
	case "UPDATE":
		keh.HandleUpdateEvent(evt)
	default:
	}
}

func (keh *KafkaEventHandler) HandleInsertEvent(evt *Entity) {
	keh.genNotifyEvent(evt)
}
func (keh *KafkaEventHandler) HandleDeleteEvent(evt *Entity) {
	keh.genNotifyEvent(evt)
}
func (keh *KafkaEventHandler) HandleUpdateEvent(evt *Entity) {
	keh.genNotifyEvent(evt)
}

type UpdateColumn struct {
	Name         string
	BeforeColumn *Column
	AfterColumn  *Column
}

func (keh *KafkaEventHandler) genNotifyEvent(evt *Entity) {
	keh.dispatcher.IncStatistic(evt.Database, evt.Table, evt.EventType)
	log.Debugf("gen notify event: %+v", evt)
	taskFieldMap := make(map[int64][]*UpdateColumn)
	for i := 0; i < len(evt.BeforeColumns); i++ {
		beforeColumn := evt.BeforeColumns[i]
		afterColumn := evt.AfterColumns[i]
		ids := keh.taskManager.GetNotifyTaskIDs(evt.Database, evt.Table, beforeColumn.Name)
		log.Debugf("%s %s %s %v", evt.Database, evt.Table, beforeColumn.Name, ids)
		for _, taskid := range ids {
			if taskFieldMap[taskid] == nil {
				taskFieldMap[taskid] = make([]*UpdateColumn, 0, 10)
			}
			updateColumn := UpdateColumn{
				Name:         beforeColumn.Name,
				BeforeColumn: beforeColumn,
				AfterColumn:  afterColumn,
			}
			taskFieldMap[taskid] = append(taskFieldMap[taskid], &updateColumn)
		}
	}
	log.Debugf("task field map: %+v", taskFieldMap)
	keh.Enqueue(evt.Database, evt.Table, evt.EventType, taskFieldMap)
}

func (keh *KafkaEventHandler) Enqueue(database, table, event string, taskFieldMap map[int64][]*UpdateColumn) {
	wg := sync.WaitGroup{}
	for taskid, fields := range taskFieldMap {
		log.Debugf("kafka event handler enqueue, task id %d, fields: %+v", taskid, fields)
		wg.Add(1)
		go func() {
			keh.enqueue(database, table, event, taskid, fields)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (keh *KafkaEventHandler) enqueue(database, table, event string, taskid int64, fields []*UpdateColumn) {
	ntyevt := new(model.NotifyEvent)
	ntyevt.Keys = make([]string, 0)
	ntyevt.Fields = make([]*model.ColumnValue, 0)
	task := keh.taskManager.GetTask(taskid)
	if task == nil {
		return
	}
	updateChanged := false
	for _, f := range fields {
		tf := keh.taskManager.GetTaskField(task, database, table, f.Name)
		if tf == nil {
			continue
		}
		if event != model.UPDATE_EVENT {
			if tf.Send == 1 {
				newValue := model.ColumnValue{
					ColunmName: f.Name,
					Value:      f.AfterColumn.Value,
					OldValue:   f.BeforeColumn.Value,
				}
				ntyevt.Fields = append(ntyevt.Fields, &newValue)
			} else {
				ntyevt.Keys = append(ntyevt.Keys, f.Name)
			}
		} else {
			// 如果该字段需要推送值，则无论是否变化都要推送，如果该字段不需要推送值，则有变化才推送
			if tf.Send == 1 {
				newValue := model.ColumnValue{
					ColunmName: f.Name,
					Value:      f.AfterColumn.Value,
					OldValue:   f.BeforeColumn.Value,
				}
				ntyevt.Fields = append(ntyevt.Fields, &newValue)
				if f.BeforeColumn.Updated {
					updateChanged = true
				}
			} else if f.BeforeColumn.Updated {
				ntyevt.Keys = append(ntyevt.Keys, f.Name)
				updateChanged = true
			}
		}
	}
	if len(ntyevt.Fields) == 0 && len(ntyevt.Keys) == 0 {
		return
	} else if event == model.UPDATE_EVENT && !updateChanged {
		return
	}
	ntyevt.Schema = database
	ntyevt.Table = table
	ntyevt.Event = event
	ntyevt.TaskID = task.ID
	name := genTaskQueueName(task)
	keh.queueManager.Enqueue(name, ntyevt)
}
