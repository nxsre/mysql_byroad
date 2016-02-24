package slave

import (
	"sync"
)

type TaskIdMap struct {
	cmap map[int64]*Task
	sync.RWMutex
}

func NewTaskIdMap(size int) *TaskIdMap {
	cmap := new(TaskIdMap)
	cmap.cmap = make(map[int64]*Task, size)
	return cmap
}

func (this *TaskIdMap) Get(id int64) *Task {
	this.RLock()
	defer this.RUnlock()
	return this.cmap[id]
}

func (this *TaskIdMap) Set(id int64, value *Task) {
	this.Lock()
	defer this.Unlock()
	this.cmap[id] = value
}

func (this *TaskIdMap) Delete(id int64) {
	this.Lock()
	defer this.Unlock()
	delete(this.cmap, id)
}
