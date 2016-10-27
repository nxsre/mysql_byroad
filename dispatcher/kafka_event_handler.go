package main

import (
	"encoding/base64"
	"fmt"
	"mysql_byroad/model"
	"mysql_byroad/mysql_schema"
	"mysql_byroad/nsq"
	"strconv"
	"sync"

	log "github.com/Sirupsen/logrus"
)

type Context struct {
	dispatcher *Dispatcher
}

type Enqueuer interface {
	Enqueue(name string, evt interface{})
}

type KafkaEventHandler struct {
	queue            Enqueuer
	taskManager      *TaskManager
	BinlogStatistics *model.BinlogStatistics
	columnManager    *schema.ColumnManager
	ctx              *Context
}

func NewKafkaEventHandler(nsqConfig NSQConf, taskManager *TaskManager, ctx *Context) (*KafkaEventHandler, error) {
	keh := &KafkaEventHandler{}
	keh.ctx = ctx
	qm, err := nsqm.GetManager(nsqConfig.LookupdHttpAddrs, nsqConfig.NsqdAddrs, nil)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	binlogStatistics := &model.BinlogStatistics{
		Statistics: make([]*model.BinlogStatistic, 0, 100),
	}
	keh.initColumnManager()
	keh.queue = qm
	keh.taskManager = taskManager
	keh.BinlogStatistics = binlogStatistics
	return keh, nil
}

func (keh *KafkaEventHandler) initColumnManager() {
	rpcClientSchema := fmt.Sprintf("%s:%d", keh.ctx.dispatcher.Config.MonitorConf.Host, keh.ctx.dispatcher.Config.MonitorConf.RpcPort)
	rpcClient := NewRPCClient(rpcClientSchema)
	dbconfigs, err := rpcClient.GetDBInstanceConfig(rpcClientSchema)
	if err != nil {
		log.Error("get db instance name error: ", err.Error())
	}
	configs := []*schema.MysqlConfig{}
	for _, config := range dbconfigs {
		myconf := schema.MysqlConfig{
			Name:     config.Name,
			Host:     config.Host,
			Port:     config.Port,
			Username: config.Username,
			Password: config.Password,
			Exclude:  config.Exclude,
			Interval: config.Interval.Duration,
		}
		configs = append(configs, &myconf)
	}
	columnManager, err := schema.NewColumnManager(configs)
	if err != nil {
		log.Errorf("new column manager error: %s", err.Error())
	}
	columnManager.BuildColumnMap()
	columnManager.LookupLoop()
	keh.columnManager = columnManager
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
	keh.BinlogStatistics.IncStatistic(evt.Database, evt.Table, evt.EventType)
	log.Debugf("gen notify event: %+v", evt)
	taskFieldMap := make(map[int64][]*UpdateColumn)
	switch toTitle(evt.EventType) {
	case model.INSERT_EVENT:
		columns := evt.AfterColumns
		for i := 0; i < len(columns); i++ {
			column := columns[i]
			keh.translateColumnValue(evt.Database, evt.Table, column)
			ids := keh.taskManager.GetNotifyTaskIDs(evt.Database, evt.Table, column.Name)
			log.Debugf("%s %s %s %v", evt.Database, evt.Table, column.Name, ids)
			for _, taskid := range ids {
				if taskFieldMap[taskid] == nil {
					taskFieldMap[taskid] = make([]*UpdateColumn, 0, 10)
				}
				updateColumn := UpdateColumn{
					Name:         column.Name,
					BeforeColumn: new(Column),
					AfterColumn:  column,
				}
				taskFieldMap[taskid] = append(taskFieldMap[taskid], &updateColumn)
			}
		}
	case model.DELETE_EVENT:
		columns := evt.BeforeColumns
		for i := 0; i < len(columns); i++ {
			column := columns[i]
			keh.translateColumnValue(evt.Database, evt.Table, column)
			ids := keh.taskManager.GetNotifyTaskIDs(evt.Database, evt.Table, column.Name)
			log.Debugf("%s %s %s %v", evt.Database, evt.Table, column.Name, ids)
			for _, taskid := range ids {
				if taskFieldMap[taskid] == nil {
					taskFieldMap[taskid] = make([]*UpdateColumn, 0, 10)
				}
				updateColumn := UpdateColumn{
					Name:         column.Name,
					BeforeColumn: new(Column),
					AfterColumn:  column,
				}
				taskFieldMap[taskid] = append(taskFieldMap[taskid], &updateColumn)
			}
		}
	case model.UPDATE_EVENT:
		for i := 0; i < len(evt.BeforeColumns); i++ {
			beforeColumn := evt.BeforeColumns[i]
			afterColumn := evt.AfterColumns[i]
			keh.translateColumnValue(evt.Database, evt.Table, beforeColumn)
			keh.translateColumnValue(evt.Database, evt.Table, afterColumn)
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
	}
	log.Debugf("task field map: %+v", taskFieldMap)
	keh.Enqueue(evt.Database, evt.Table, evt.EventType, taskFieldMap)
}

func (keh *KafkaEventHandler) Enqueue(database, table, event string, taskFieldMap map[int64][]*UpdateColumn) {
	wg := sync.WaitGroup{}
	for taskid, fields := range taskFieldMap {
		log.Debugf("kafka event handler enqueue, task id %d, fields: %+v", taskid, fields)
		wg.Add(1)
		go func(id int64, fs []*UpdateColumn) {
			keh.enqueue(database, table, event, id, fs)
			wg.Done()
		}(taskid, fields)
	}
	wg.Wait()
}

func (keh *KafkaEventHandler) enqueue(database, table, event string, taskid int64, fields []*UpdateColumn) {
	event = toTitle(event)
	log.Debugf("enqueue: %s.%s %s %d: %+v -> %+v", database, table, event, taskid, fields[0].BeforeColumn, fields[0].AfterColumn)
	ntyevt := new(model.NotifyEvent)
	ntyevt.Keys = make([]string, 0)
	ntyevt.Fields = make([]*model.ColumnValue, 0)
	task := keh.taskManager.GetTask(taskid)
	if task == nil {
		return
	}
	updateChanged := false
	//shit
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
				if f.AfterColumn.Updated {
					updateChanged = true
				}
			} else if f.AfterColumn.Updated {
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
	keh.queue.Enqueue(name, ntyevt)
}

/*
根据字段类型，得到和接binlog相兼容的数据
*/
func (keh *KafkaEventHandler) translateColumnValue(schema, table string, column *Column) {
	myColumn := keh.columnManager.GetColumnByName(schema, table, column.Name)
	if myColumn != nil {
		if myColumn.IsEnum() {
			index, err := strconv.Atoi(column.Value)
			if err != nil {
				return
			}
			enumValue := myColumn.GetEnumValue(index)
			column.Value = enumValue
		} else if myColumn.IsText() || myColumn.IsBlob() {
			data := []byte(column.Value)
			column.Value = base64.StdEncoding.EncodeToString(data)
		}
	}
}
