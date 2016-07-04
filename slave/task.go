package slave

import "mysql_byroad/model"

type TaskSlice []*model.Task

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

func initNotifyAPIDB() {
	model.CreateConfigTable()
	model.CreateTaskTable()
	model.CreateNotifyFieldTable()
	model.CreateStatisticTable()
	taskIdcmap = _selectAllTasks()
	ntytasks = NewNotifyTaskMap(taskIdcmap)
}

func GetTask(id int64) *model.Task {
	return taskIdcmap.Get(id)
}

func cleanRedisEvent(task *model.Task) {
	name := genTaskQueueName(task)
	rename := genTaskReQueueName(task)
	queueManager.Empty(name)
	queueManager.Empty(rename)
}

func deleteTask(task *model.Task) error {
	cleanRedisEvent(task)
	return task.Delete()
}

func getTaskField(task *model.Task, schema, table, column string) *model.NotifyField {
	for _, field := range task.Fields {
		if isSchemaMatch(field.Schema, schema) && isTableMatch(field.Table, table) && field.Column == column {
			return field
		}
	}
	return nil
}

/*
读取数据库中的task和field，将其放入内存的taskMap中
*/
func _selectAllTasks() *TaskIdMap {
	tasks := NewTaskIdMap(100)
	s := "SELECT `id`, `name`, `apiurl`, `event`, `stat`, `create_time`, `create_user`,`routine_count`, `re_routine_count`, `re_send_time`, `retry_count`, `timeout`, `desc`, `pack_protocal` FROM `task`"
	stmt, err := confdb.Prepare(s)
	defer stmt.Close()
	sysLogger.LogErr(err)
	if err != nil {
		return tasks
	}
	rows, err := stmt.Query()
	sysLogger.LogErr(err)
	if err != nil {
		return tasks
	}
	for rows.Next() {
		t := new(model.Task)
		rows.Scan(&t.ID, &t.Name, &t.Apiurl, &t.Event, &t.Stat, &t.CreateTime, &t.CreateUser, &t.RoutineCount, &t.ReRoutineCount, &t.ReSendTime, &t.RetryCount, &t.Timeout, &t.Desc, &t.PackProtocal)
		tasks.Set(t.ID, t)
	}
	s = "SELECT `id`, `schema`, `table`, `column`, `send`, `task_id` FROM `notify_field`"
	stmt, err = confdb.Prepare(s)
	sysLogger.LogErr(err)
	if err != nil {
		return tasks
	}
	rows, err = stmt.Query()
	sysLogger.LogErr(err)
	if err != nil {
		return tasks
	}
	for rows.Next() {
		f := new(model.NotifyField)
		rows.Scan(&f.ID, &f.Schema, &f.Table, &f.Column, &f.Send, &f.TaskID)
		task := tasks.Get(f.TaskID)
		if task != nil {
			if task.Fields == nil {
				task.Fields = make([]*model.NotifyField, 0, 10)
			}
			task.Fields = append(task.Fields, f)
		}
	}
	return tasks
}
