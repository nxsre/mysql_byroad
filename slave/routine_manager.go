package slave

import (
	"mysql_byroad/common"
	"mysql_byroad/gorpool"
	"mysql_byroad/model"
)

type RoutineManager struct {
	taskRoutinePools   []*gorpool.RoutinePool
	taskReRoutinePools []*gorpool.RoutinePool
}

func NewRoutineManager() *RoutineManager {
	rp := make([]*gorpool.RoutinePool, 0, 100)
	rrp := make([]*gorpool.RoutinePool, 0, 100)
	return &RoutineManager{
		taskRoutinePools:   rp,
		taskReRoutinePools: rrp,
	}
}

/*
根据系统配置，生成相应数量的协程，分别处理消息推送和消息的重推
*/
func (this *RoutineManager) InitTaskRoutines() {
	for _, task := range taskIdcmap.cmap {
		if task.Stat == common.TASK_STATE_START {
			this.AddTaskRoutines(task)
		} else {
			this.AddStopTaskRoutines(task)
		}
	}
}

/*
为单个任务生成相应数量的协程
*/
func (this *RoutineManager) AddTaskRoutines(task *model.Task) {
	name := genTaskQueueName(task)
	this.taskRoutinePools = append(this.taskRoutinePools, gorpool.NewPool(task.RoutineCount, notifyRoutine, name))
	rename := genTaskReQueueName(task)
	this.taskReRoutinePools = append(this.taskReRoutinePools, gorpool.NewPool(task.ReRoutineCount, notifyRetryRoutine, rename))
}

func (this *RoutineManager) AddStopTaskRoutines(task *model.Task) {
	name := genTaskQueueName(task)
	this.taskRoutinePools = append(this.taskRoutinePools, gorpool.NewPool(0, notifyRoutine, name))
	rename := genTaskReQueueName(task)
	this.taskReRoutinePools = append(this.taskReRoutinePools, gorpool.NewPool(0, notifyRetryRoutine, rename))
}

/*
根据任务获得相应的推送消息的协程池
*/
func (this *RoutineManager) getRoutinePool(task *model.Task) *gorpool.RoutinePool {
	name := genTaskQueueName(task)
	for _, pool := range this.taskRoutinePools {
		if pool.GetName() == name {
			return pool
		}
	}
	return nil
}

/*
根据任务获得相应的重推消息的协程池
*/
func (this *RoutineManager) getReRoutinePool(task *model.Task) *gorpool.RoutinePool {
	name := genTaskReQueueName(task)
	for _, pool := range this.taskReRoutinePools {
		if pool.GetName() == name {
			return pool
		}
	}
	return nil
}

/*
清楚任务的协程池
*/
func (this *RoutineManager) CleanTaskRoutinePool(task *model.Task) {
	this.cleanRoutinePool(task)
	this.cleanReRoutinePool(task)
}

func (this *RoutineManager) cleanRoutinePool(task *model.Task) {
	name := genTaskQueueName(task)
	for i, pool := range this.taskRoutinePools {
		if pool.GetName() == name {
			this.taskRoutinePools[i] = nil
			return
		}
	}
}

func (this *RoutineManager) cleanReRoutinePool(task *model.Task) {
	name := genTaskReQueueName(task)
	for i, pool := range this.taskReRoutinePools {
		if pool.GetName() == name {
			this.taskReRoutinePools[i] = nil
			return
		}
	}
}

/*
更新任务的协程数
*/
func (this *RoutineManager) UpdateTaskRoutine(task *model.Task) {
	this.updateRoutines(task)
	this.updateReRoutines(task)
}

func (this *RoutineManager) updateRoutines(task *model.Task) {
	rp := this.getRoutinePool(task)
	if rp != nil {
		num := task.RoutineCount
		rp.ChangeTo(num)
	}
}

func (this *RoutineManager) updateReRoutines(task *model.Task) {
	rp := this.getReRoutinePool(task)
	if rp != nil {
		num := task.ReRoutineCount
		rp.ChangeTo(num)
	}
}

/*
停止任务的所有协程
*/
func (this *RoutineManager) StopTaskRoutine(task *model.Task) {
	this.stopRoutines(task)
	this.stopReRoutines(task)
}

func (this *RoutineManager) stopRoutines(task *model.Task) {
	rp := this.getRoutinePool(task)
	if rp != nil {
		rp.Clean()
		rp.Wait()
	}
}

func (this *RoutineManager) stopReRoutines(task *model.Task) {
	rp := this.getReRoutinePool(task)
	if rp != nil {
		rp.Clean()
		rp.Wait()
	}
}

func (this *RoutineManager) StartTaskRoutine(task *model.Task) {
	rp := this.getRoutinePool(task)
	if rp != nil {
		rp.ChangeTo(task.RoutineCount)
	}
	rrp := this.getReRoutinePool(task)
	if rrp != nil {
		rrp.ChangeTo(task.ReRoutineCount)
	}
}

func (this *RoutineManager) Clean() {
	for _, pool := range this.taskRoutinePools {
		pool.Clean()
	}
	for _, pool := range this.taskReRoutinePools {
		pool.Clean()
	}
	for _, pool := range this.taskRoutinePools {
		pool.Wait()
	}
	for _, pool := range this.taskReRoutinePools {
		pool.Wait()
	}
}
