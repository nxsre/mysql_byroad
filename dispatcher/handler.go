package main

import (
	"fmt"
	"mysql_byroad/model"

	log "github.com/Sirupsen/logrus"
	"github.com/siddontang/go-mysql/replication"
)

type RowsEventHandler struct {
}

/*
	对row格式的数据进行处理
*/
func NewRowsEventHandler(conf MysqlConf) *RowsEventHandler {
	reh := &RowsEventHandler{}
	return reh
}

func (reh *RowsEventHandler) HandleEvent(ev *replication.BinlogEvent) {
	switch e := ev.Event.(type) {
	case *replication.RowsEvent:
		binlogInfo.Position = ev.Header.LogPos
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
	case *replication.RotateEvent:
		binlogInfo.Filename = string(e.NextLogName)
		binlogInfo.Position = uint32(e.Position)
	}
}

func (eh *RowsEventHandler) HandleWriteEvent(e *replication.RowsEvent) {
	log.Info("handle write event")
	event := model.INSERT_EVENT
	schema, table := string(e.Table.Schema), string(e.Table.Table)
	if !taskManager.InNotifyTable(schema, table) {
		return
	}
	for _, row := range e.Rows {
		columns := []*model.ColumnValue{}
		binlogStatistics.IncStatistic(schema, table, event)
		for j, r := range row {
			column := columnManager.GetColumnName(schema, table, j)
			if taskManager.InNotifyField(schema, table, column) {
				c := new(model.ColumnValue)
				c.ColunmName = column
				switch t := r.(type) {
				case int, int16, int32, int64:
					c.Value = fmt.Sprintf("%v", t)
				default:
					c.Value = r
				}
				columns = append(columns, c)
			}
		}
		eh.genNotifyEvents(schema, table, columns, event)
	}
}

func (eh *RowsEventHandler) HandleDeleteEvent(e *replication.RowsEvent) {
	log.Info("handle delete event")
	event := model.DELETE_EVENT
	schema, table := string(e.Table.Schema), string(e.Table.Table)
	if !taskManager.InNotifyTable(schema, table) {
		return
	}
	for _, row := range e.Rows {
		columns := []*model.ColumnValue{}
		binlogStatistics.IncStatistic(schema, table, event)
		for j, r := range row {
			column := columnManager.GetColumnName(schema, table, j)
			if taskManager.InNotifyField(schema, table, column) {
				c := new(model.ColumnValue)
				c.ColunmName = column
				//c.Value = r
				switch t := r.(type) {
				case int, int16, int32, int64:
					c.Value = fmt.Sprintf("%v", t)
				default:
					c.Value = r
				}
				columns = append(columns, c)
			}
		}
		eh.genNotifyEvents(schema, table, columns, event)
	}
}

func (eh *RowsEventHandler) HandleUpdateEvent(e *replication.RowsEvent) {
	log.Info("handle update event")
	event := model.UPDATE_EVENT
	schema, table := string(e.Table.Schema), string(e.Table.Table)
	if !taskManager.InNotifyTable(schema, table) {
		return
	}
	oldRows, newRows := getUpdateRows(e)
	for i := 0; i < len(oldRows) && i < len(newRows); i++ {
		columns := []*model.ColumnValue{}
		oldRow := oldRows[i]
		newRow := newRows[i]
		binlogStatistics.IncStatistic(schema, table, event)
		for j := 0; j < len(oldRow) && j < len(newRow); j++ {
			column := columnManager.GetColumnName(schema, table, j)
			if taskManager.InNotifyField(schema, table, column) {
				c := new(model.ColumnValue)
				c.ColunmName = column
				switch t := newRow[j].(type) {
				case int, int16, int32, int64:
					c.Value = fmt.Sprintf("%v", t)
				default:
					c.Value = newRow[j]
				}
				switch t := oldRow[j].(type) {
				case int, int16, int32, int64:
					c.OldValue = fmt.Sprintf("%v", t)
				default:
					c.OldValue = oldRow[j]
				}
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
	log.Infof("gen notify event: %s %s %s %v", event, schema, table, columns)
	//为相应的任务添加订阅了的字段
	taskFieldMap := make(map[int64][]*model.ColumnValue)
	for _, column := range columns {
		ids := taskManager.GetNotifyTaskIDs(schema, table, column.ColunmName)
		log.Debug("ids ", ids)
		for _, taskID := range ids {
			if taskFieldMap[taskID] == nil {
				taskFieldMap[taskID] = make([]*model.ColumnValue, 0)
			}
			taskFieldMap[taskID] = append(taskFieldMap[taskID], column)
		}
	}
	eh.Enqueue(schema, table, event, taskFieldMap)
}
