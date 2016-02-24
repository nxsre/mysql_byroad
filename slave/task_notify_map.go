package slave

import (
	"mysql-slave/common"
	"sync"
)

type TaskMatchMap map[string]map[string]map[string][]int64

/*
`数据库-表-字段`对应的所有推送的taskID
*/
type NotifyTaskMap struct {
	notifyTasks TaskMatchMap
	sync.RWMutex
}

/*
生成`数据库-表-字段`对应所有要推送的task id
*/
func NewNotifyTaskMap(tasks *TaskIdMap) *NotifyTaskMap {
	ntytaskmap := new(NotifyTaskMap)
	ntytaskmap.notifyTasks = createTaskMap(tasks)
	return ntytaskmap
}

func createTaskMap(tasks *TaskIdMap) TaskMatchMap {
	tmm := make(TaskMatchMap)
	for _, task := range tasks.cmap {
		if task.Stat == common.TASK_STATE_START {
			addToTaskNoitfyMap(tmm, task)
		}
	}
	return tmm
}

/*
根据`数据库-表-字段`获得要推送的所有任务ID
*/
func (this *NotifyTaskMap) GetNotifyTaskIDs(schema, table, column string) []int64 {
	this.RLock()
	defer this.RUnlock()
	cmap := this.notifyTasks
	if cmap != nil && cmap[schema] != nil && cmap[schema][table] != nil && cmap[schema][table][column] != nil {
		return cmap[schema][table][column]
	}
	return nil
}

/*
更新taskmap
*/
func (this *NotifyTaskMap) UpdateNotifyTaskMap(tasks *TaskIdMap) {
	tmap := createTaskMap(tasks)
	this.Lock()
	defer this.Unlock()
	this.notifyTasks = tmap
}

func (this *NotifyTaskMap) AddTask(task *Task) {
	this.Lock()
	defer this.Unlock()
	addToTaskNoitfyMap(this.notifyTasks, task)
}

func addToTaskNoitfyMap(cmap TaskMatchMap, task *Task) {
	for _, field := range task.Fields {
		schema, table, column := field.Schema, field.Table, field.Column
		if cmap[schema] == nil {
			cmap[schema] = make(map[string]map[string][]int64)
		}
		if cmap[schema][table] == nil {
			cmap[schema][table] = make(map[string][]int64)
		}
		if cmap[schema][table][column] == nil {
			cmap[schema][table][column] = make([]int64, 0, 100)
		}
		cmap[schema][table][column] = append(cmap[schema][table][column], task.ID)
	}
}

func (this *NotifyTaskMap) InNotifyTable(schema, table string) bool {
	this.RLock()
	defer this.RUnlock()
	tmap := this.notifyTasks
	if tmap != nil && tmap[schema] != nil && tmap[schema][table] != nil {
		return true
	}
	return false
}

func (this *NotifyTaskMap) InNotifyField(schema, table, column string) bool {
	this.RLock()
	defer this.RUnlock()
	cmap := this.notifyTasks
	if cmap != nil && cmap[schema] != nil && cmap[schema][table] != nil && cmap[schema][table][column] != nil {
		return true
	}
	return false
}
