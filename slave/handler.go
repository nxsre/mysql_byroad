package slave

import (
	"fmt"
	"mysql_byroad/model"

	"github.com/siddontang/go-mysql/replication"
	"mysql_byroad/common"
)

/*
Insert事件的处理
*/
func handleWriteEvent(e *replication.RowsEvent) {
	event := common.INSERT_EVENT
	schema, table := string(e.Table.Schema), string(e.Table.Table)
	if !inNotifyTable(schema, table) {
		return
	}
	for _, row := range e.Rows {
		columns := []*model.ColumnValue{}
		binlogStatistics.IncStatistic(schema, table, event)
		for j, r := range row {
			column := columnManager.GetColumnName(schema, table, j)
			if inNotifyFields(schema, table, column) {
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
		genNotifyEvents(schema, table, columns, event)
	}
}

/*
delete事件的处理
*/
func handleDeleteEvent(e *replication.RowsEvent) {
	event := common.DELETE_EVENT
	schema, table := string(e.Table.Schema), string(e.Table.Table)
	if !inNotifyTable(schema, table) {
		return
	}
	for _, row := range e.Rows {
		columns := []*model.ColumnValue{}
		binlogStatistics.IncStatistic(schema, table, event)
		for j, r := range row {
			column := columnManager.GetColumnName(schema, table, j)
			if inNotifyFields(schema, table, column) {
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
		genNotifyEvents(schema, table, columns, event)
	}
}

/*
update事件的处理
*/
func handleUpdateEvent(e *replication.RowsEvent) {
	event := common.UPDATE_EVENT
	schema, table := string(e.Table.Schema), string(e.Table.Table)
	if !inNotifyTable(schema, table) {
		return
	}
	oldRows, newRows := getUpdateRows(e)
	for i := 0; i < len(oldRows) && i < len(newRows); i++ {
		columns := []*model.ColumnValue{}
		binlogStatistics.IncStatistic(schema, table, event)
		oldRow := oldRows[i]
		newRow := newRows[i]
		for j := 0; j < len(oldRow) && j < len(newRow); j++ {
			column := columnManager.GetColumnName(schema, table, j)
			if inNotifyFields(schema, table, column) {
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
		genNotifyEvents(schema, table, columns, event)
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
判断 `数据库-表-字段` 是否在需要推送的任务里
*/
func inNotifyFields(schema, table, column string) bool {
	return ntytasks.InNotifyField(schema, table, column)
}

/*
判断数据库-表是否有任务订阅，并且要推送
*/
func inNotifyTable(schema, table string) bool {
	return ntytasks.InNotifyTable(schema, table)
}

/*
根据`数据库-表-字段` 匹配订阅了该字段的任务，为每个任务生成相应的消息，放入推送消息队列中
*/
func genNotifyEvents(schema, table string, columns []*model.ColumnValue, event string) {
	//为相应的任务添加订阅了的字段
	taskFieldMap := make(map[int64][]*model.ColumnValue)
	for _, column := range columns {
		ids := ntytasks.GetNotifyTaskIDs(schema, table, column.ColunmName)
		for _, taskID := range ids {
			if taskFieldMap[taskID] == nil {
				taskFieldMap[taskID] = make([]*model.ColumnValue, 0)
			}
			taskFieldMap[taskID] = append(taskFieldMap[taskID], column)
		}
	}
	eventEnqueuer.Enqueue(schema, table, event, taskFieldMap)
}
