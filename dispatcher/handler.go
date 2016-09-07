package main

import (
	"fmt"
	"mysql_byroad/model"
	"mysql_byroad/mysql_schema"

	log "github.com/Sirupsen/logrus"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"
)

type RowsEventHandler struct {
	eventEnqueuer     *EventEnqueuer
	replicationClient *ReplicationClient
	taskManager       *TaskManager
	dispatcher        *Dispatcher
}

/*
	对row格式的数据进行处理
*/
func NewRowsEventHandler(ctx context.Context) *RowsEventHandler {
	disp := ctx.Value("dispatcher").(*Dispatcher)
	reh := &RowsEventHandler{}
	eventEnqueuer := NewEventEnqueuer(ctx)
	reh.eventEnqueuer = eventEnqueuer
	reh.dispatcher = disp
	reh.replicationClient = disp.replicationClient
	reh.taskManager = disp.taskManager
	return reh
}

func (reh *RowsEventHandler) HandleEvent(ev *replication.BinlogEvent) {
	switch e := ev.Event.(type) {
	case *replication.RowsEvent:
		switch ev.Header.EventType {
		case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
			reh.HandleWriteEvent(e)
		case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			reh.HandleDeleteEvent(e)
		case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			reh.HandleUpdateEvent(e)
		default:
			log.Info("Event type %s not supported", ev.Header.EventType)
		}
		reh.replicationClient.binlogInfo.Position = ev.Header.LogPos
	case *replication.RotateEvent:
		reh.replicationClient.binlogInfo.Filename = string(e.NextLogName)
		reh.replicationClient.binlogInfo.Position = uint32(e.Position)
	}
}

func (eh *RowsEventHandler) HandleWriteEvent(e *replication.RowsEvent) {
	log.Debug("handle write event")
	event := model.INSERT_EVENT
	schema, table := string(e.Table.Schema), string(e.Table.Table)
	if !eh.taskManager.InNotifyTable(schema, table) {
		return
	}
	for _, row := range e.Rows {
		columns := []*model.ColumnValue{}
		eh.dispatcher.IncStatistic(schema, table, event)
		for j, r := range row {
			column := eh.replicationClient.columnManager.GetColumn(schema, table, j)
			if column == nil {
				log.Error("column not exists: %s.%s.%d", schema, table, j)
				continue
			}
			if eh.taskManager.InNotifyField(schema, table, column.Name) {
				c := getColumnValue(nil, r, column)
				columns = append(columns, c)
			}
		}
		eh.genNotifyEvents(schema, table, columns, event)
	}
}

func (eh *RowsEventHandler) HandleDeleteEvent(e *replication.RowsEvent) {
	log.Debug("handle delete event")
	event := model.DELETE_EVENT
	schema, table := string(e.Table.Schema), string(e.Table.Table)
	if !eh.taskManager.InNotifyTable(schema, table) {
		return
	}
	for _, row := range e.Rows {
		columns := []*model.ColumnValue{}
		eh.dispatcher.IncStatistic(schema, table, event)
		for j, r := range row {
			column := eh.replicationClient.columnManager.GetColumn(schema, table, j)
			if column == nil {
				log.Error("column not exists: %s.%s.%d", schema, table, j)
				continue
			}
			if eh.taskManager.InNotifyField(schema, table, column.Name) {
				c := getColumnValue(nil, r, column)
				columns = append(columns, c)
			}
		}
		eh.genNotifyEvents(schema, table, columns, event)
	}
}

func (eh *RowsEventHandler) HandleUpdateEvent(e *replication.RowsEvent) {
	log.Debug("handle update event")
	event := model.UPDATE_EVENT
	schema, table := string(e.Table.Schema), string(e.Table.Table)
	if !eh.taskManager.InNotifyTable(schema, table) {
		return
	}
	oldRows, newRows := getUpdateRows(e)
	for i := 0; i < len(oldRows) && i < len(newRows); i++ {
		columns := []*model.ColumnValue{}
		oldRow := oldRows[i]
		newRow := newRows[i]
		eh.dispatcher.IncStatistic(schema, table, event)
		for j := 0; j < len(oldRow) && j < len(newRow); j++ {
			column := eh.replicationClient.columnManager.GetColumn(schema, table, j)
			if column == nil {
				log.Error("column not exists: %s.%s.%d", schema, table, j)
				continue
			}
			if eh.taskManager.InNotifyField(schema, table, column.Name) {
				c := getColumnValue(oldRow[j], newRow[j], column)
				columns = append(columns, c)
			}
		}
		eh.genNotifyEvents(schema, table, columns, event)
	}
}

func getUpdateRows(e *replication.RowsEvent) (oldRows [][]interface{}, newRows [][]interface{}) {
	for i := 0; i < len(e.Rows); i += 2 {
		oldRows = append(oldRows, e.Rows[i])
		newRows = append(newRows, e.Rows[i+1])
	}
	return
}

/*
根据`数据库-表-字段` 匹配订阅了该字段的任务，为每个任务生成相应的消息，放入推送消息队列中
*/
func (eh *RowsEventHandler) genNotifyEvents(schema, table string, columns []*model.ColumnValue, event string) {
	log.Debugf("gen notify event: %s %s %s %v", event, schema, table, columns)
	//为相应的任务添加订阅了的字段
	taskFieldMap := make(map[int64][]*model.ColumnValue)
	for _, column := range columns {
		ids := eh.taskManager.GetNotifyTaskIDs(schema, table, column.ColunmName)
		log.Debugf("%s %s %s %d", schema, table, column.ColunmName, ids)
		for _, taskID := range ids {
			if taskFieldMap[taskID] == nil {
				taskFieldMap[taskID] = make([]*model.ColumnValue, 0)
			}
			taskFieldMap[taskID] = append(taskFieldMap[taskID], column)
		}
	}
	log.Debugf("task field map: %v", taskFieldMap)
	eh.Enqueue(schema, table, event, taskFieldMap)
}

func getColumnValue(old interface{}, newv interface{}, column *schema.Column) *model.ColumnValue {
	c := &model.ColumnValue{}
	c.ColunmName = column.Name
	if old != nil {
		c.OldValue = getValue(old, column)
	}
	if newv != nil {
		c.Value = getValue(newv, column)
	}
	return c
}

/*
根据字段的类型得到相应的值，需要对整型的数据进行转换
*/
func getValue(v interface{}, column *schema.Column) interface{} {
	var value interface{}
	switch t := v.(type) {
	case int8:
		if column.IsUnsigned() {
			v := uint8(t)
			value = fmt.Sprintf("%d", v)
		} else {
			value = fmt.Sprintf("%d", t)
		}
	case int16:
		if column.IsUnsigned() {
			v := uint16(t)
			value = fmt.Sprintf("%d", v)
		} else {
			value = fmt.Sprintf("%d", t)
		}
	case int32:
		if column.IsUnsigned() {
			v := uint32(t)
			value = fmt.Sprintf("%d", v)
		} else {
			value = fmt.Sprintf("%d", t)
		}
	case int64:
		if column.IsUnsigned() {
			v := uint64(t)
			value = fmt.Sprintf("%d", v)
		} else {
			value = fmt.Sprintf("%d", t)
		}
	default:
		value = t
	}
	return value
}
