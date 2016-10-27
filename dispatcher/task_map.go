package main

import (
	"mysql_byroad/model"
	"sync"
)

type TaskIdMap struct {
	cmap map[int64]*model.Task
	sync.RWMutex
}

func NewTaskIdMap(size int) *TaskIdMap {
	cmap := new(TaskIdMap)
	cmap.cmap = make(map[int64]*model.Task, size)
	return cmap
}

func (this *TaskIdMap) Get(id int64) *model.Task {
	this.RLock()
	defer this.RUnlock()
	return this.cmap[id]
}

func (this *TaskIdMap) Set(id int64, value *model.Task) {
	this.Lock()
	defer this.Unlock()
	this.cmap[id] = value
}

func (this *TaskIdMap) Delete(id int64) {
	this.Lock()
	defer this.Unlock()
	delete(this.cmap, id)
}

type TaskMap struct {
	cmap map[string]*model.Task
	sync.RWMutex
}

func NewTaskMap(size int) *TaskMap {
	tmap := new(TaskMap)
	tmap.cmap = make(map[string]*model.Task, size)
	return tmap
}

func (this *TaskMap) Get(name string) *model.Task {
	this.RLock()
	task := this.cmap[name]
	this.RUnlock()
	return task
}

func (this *TaskMap) Set(name string, value *model.Task) {
	this.Lock()
	this.cmap[name] = value
	this.Unlock()
}

func (this *TaskMap) Delete(name string) {
	this.Lock()
	delete(this.cmap, name)
	this.Unlock()
}
