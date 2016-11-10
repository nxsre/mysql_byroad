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

func (this *TaskIdMap) Iter() <-chan *model.Task {
	ch := make(chan *model.Task)
	go func(c chan *model.Task) {
		this.RLock()
		for _, task := range this.cmap {
			c <- task
		}
		this.RUnlock()
		close(c)
	}(ch)
	return ch
}

func (this *TaskIdMap) IterBuffered() <-chan *model.Task {
	ch := make(chan *model.Task, len(this.cmap))
	go func(c chan *model.Task) {
		this.RLock()
		for _, task := range this.cmap {
			c <- task
		}
		this.RUnlock()
		close(c)
	}(ch)
	return ch
}
