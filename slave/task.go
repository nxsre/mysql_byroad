package slave

import (
	"mysql_byroad/common"

	"github.com/jmoiron/sqlx"
)

type TaskSlice []*Task

func (t TaskSlice) Len() int {
	return len(t)
}

func (t TaskSlice) Less(i, j int) bool {
	return int64(t[i].CreateTime.Sub(t[j].CreateTime)) > 0
}

func (t TaskSlice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

//所有任务的map格式，key是任务的ID，value是Task对象，Task对象中包含其fields对象
var taskIdcmap *TaskIdMap
var ntytasks *NotifyTaskMap

func initNotifyAPIDB(confdb *sqlx.DB) {
	createConfigTable(confdb)
	createTaskTable(confdb)
	createNotifyFieldTable(confdb)
	createStaticTable(confdb)
	taskIdcmap = _selectAllTasks(confdb)
	ntytasks = NewNotifyTaskMap(taskIdcmap)
}

func (t *Task) GetField(schema, table, column string) *NotifyField {
	for _, field := range t.Fields {
		if field.Schema == schema && field.Table == table && field.Column == column {
			return field
		}
	}
	return nil
}

func (task *Task) Add() (id int64, err error) {
	id, err = task._insert(confdb)
	if err != nil {
		sysLogger.Log(err.Error())
		owl.LogThisException(err.Error())
		return
	}
	err = task.Fields._insert(id, confdb)
	if err != nil {
		sysLogger.Log(err.Error())
		owl.LogThisException(err.Error())
		return
	}
	task.ID = id
	taskIdcmap.Set(id, task)
	if task.Stat == common.TASK_STATE_START {
		ntytasks.AddTask(task)
		routineManager.AddTaskRoutines(task)
	} else if task.Stat == common.TASK_STATE_STOP {
		routineManager.AddStopTaskRoutines(task)
	}
	return
}

func GetTask(id int64) *Task {
	return taskIdcmap.Get(id)
}

func (task *Task) SetStat() error {
	stat := task.Stat
	_, err := task._update(confdb)
	if err != nil {
		sysLogger.Log(err.Error())
		owl.LogThisException(err.Error())
		return err
	}
	ntytasks.UpdateNotifyTaskMap(taskIdcmap)
	if stat == common.TASK_STATE_START {
		routineManager.StartTaskRoutine(task)
	} else if stat == common.TASK_STATE_STOP {
		routineManager.StopTaskRoutine(task)
	}
	return nil
}

func (task *Task) Update() error {
	taskIdcmap.Set(task.ID, task)
	_, err := task._update(confdb)
	if err != nil {
		sysLogger.Log(err.Error())
		owl.LogThisException(err.Error())
		return err
	}
	ntytasks.UpdateNotifyTaskMap(taskIdcmap)
	if task.Stat == common.TASK_STATE_START {
		routineManager.UpdateTaskRoutine(task)
	}

	return nil
}

func (task *Task) Delete() error {
	taskIdcmap.Delete(task.ID)
	ntytasks.UpdateNotifyTaskMap(taskIdcmap)
	routineManager.StopTaskRoutine(task)
	task.cleanRedisEvent()
	_, err := task._delete(confdb)
	if err != nil {
		sysLogger.Log(err.Error())
		owl.LogThisException(err.Error())
	}
	return err
}

func (task *Task) cleanRedisEvent() {
	name := genTaskQueueName(task)
	rename := genTaskReQueueName(task)
	queueManager.Empty(name)
	queueManager.Empty(rename)
}

func (this *Task) FieldExists(field *NotifyField) bool {
	for _, f := range this.Fields {
		if f.Schema == field.Schema && f.Table == field.Table && f.Column == field.Column {
			return true
		}
	}
	return false
}

func (task *Task) GetTaskColumnsMap() map[string]map[string][]*NotifyField {
	colsMap := make(map[string]map[string][]*NotifyField)
	for _, field := range task.Fields {
		if colsMap[field.Schema] == nil {
			colsMap[field.Schema] = make(map[string][]*NotifyField)
			colsMap[field.Schema][field.Table] = make([]*NotifyField, 0)
		}
		colsMap[field.Schema][field.Table] = append(colsMap[field.Schema][field.Table], field)
	}
	return colsMap
}
