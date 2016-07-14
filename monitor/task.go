package main

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
