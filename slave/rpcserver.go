package slave

import (
	"errors"
	"fmt"
	"mysql-slave/common"
	"net"
	"net/http"
	"net/rpc"
	"sort"
)

type ByRoad struct {
	protocol string
	schema   string
}

func NewRPCServer(protocol, schema string) *ByRoad {
	byroad := ByRoad{
		protocol: protocol,
		schema:   schema,
	}
	return &byroad
}

func (this *ByRoad) Start() {
	rpc.Register(this)
	rpc.HandleHTTP()
	l, e := net.Listen(this.protocol, this.schema)
	if e != nil {
		panic(e.Error())
	}
	go http.Serve(l, nil)
}
func (b *ByRoad) GetTask(taskid int64, task *Task) error {
	t := GetTask(taskid)
	if t == nil {
		return errors.New("task not found")
	}
	*task = *t
	return nil
}

func (b *ByRoad) GetTasks(username string, tasks *[]*Task) error {
	taskIdcmap.RLock()
	for _, t := range taskIdcmap.cmap {
		if username == t.CreateUser {
			*tasks = append(*tasks, t)
		}
	}
	taskIdcmap.RUnlock()
	sort.Sort(TaskSlice(*tasks))
	queueManager.TasksQueueLen(*tasks)
	return nil
}

func (b *ByRoad) GetAllTasks(username string, tasks *[]*Task) error {
	for _, t := range taskIdcmap.cmap {
		*tasks = append(*tasks, t)
	}
	sort.Sort(TaskSlice(*tasks))
	queueManager.TasksQueueLen(*tasks)
	return nil
}

func (b *ByRoad) AddTask(task *Task, status *string) error {
	_, err := task.Add()
	if err != nil {
		*status = "fail"
		return err
	}
	*status = "sucess"
	return nil
}

func (b *ByRoad) DeleteTask(taskid int64, status *string) error {
	task := GetTask(taskid)
	if task == nil {
		return errors.New("not found")
	}
	err := task.Delete()
	if err != nil {
		*status = "fail"
		return err
	}
	*status = "success"
	return nil
}

func (b *ByRoad) UpdateTask(task *Task, status *string) error {
	err := task.Update()
	if err != nil {
		*status = "fail"
		return err
	}
	*status = "success"
	return nil
}

func (b *ByRoad) ChangeTaskStat(task *Task, status *string) error {
	t := GetTask(task.ID)
	t.Stat = task.Stat
	err := t.SetStat()
	fmt.Println(t)
	if err != nil {
		*status = "fail"
		return err
	}
	*status = "success"
	return nil
}

func (b *ByRoad) GetColumns(username string, columns *common.OrderedSchemas) error {
	*columns = columnManager.GetOrderedColumns()
	return nil
}

func (b *ByRoad) GetTaskColumns(task *Task, columns *map[string]map[string][]*NotifyField) error {
	*columns = task.GetTaskColumnsMap()
	return nil
}

/*
func (b *ByRoad) GetConfigMap(username string, configs *[]*Config) error {
	configlist := make([]*Config, 0, 10)
	for _, config := range configMap {
		configlist = append(configlist, config)
	}
	sort.Sort(ConfigSlice(configlist))
	*configs = configlist
	return nil
}
*/
func (b *ByRoad) TaskExists(task *Task, reply *bool) error {
	return nil
}

func (b *ByRoad) TaskNameExists(name string, reply *bool) error {
	for _, task := range taskIdcmap.cmap {
		if task.Name == name {
			*reply = true
			return nil
		}
	}
	*reply = false
	return nil
}

func (b *ByRoad) TasksQueueLen(tasks []*Task, results *[][]int64) error {
	*results = queueManager.TasksQueueLen(tasks)
	return nil
}

/*
func (b *ByRoad) UpdateColumns(username string, columns *OrderedSchemas) error {
	reloadColumnsMap()
	*columns = columnManager.GetOrderedColumns()
	return nil
}

func (b *ByRoad) GetStatus(username string, st *map[string]interface{}) error {
	start := startTime
	duration := time.Now().Sub(start)
	statusMap := make(map[string]interface{})
	statusMap["sendEventCount"] = sendEventCount
	statusMap["resendEventCount"] = resendEventCount
	statusMap["sendSuccessEventCount"] = sendSuccessEventCount
	statusMap["sendFailedEventCount"] = sendFailedEventCount
	statusMap["Start"] = start.String()
	statusMap["Duration"] = duration.String()
	statusMap["routineNumber"] = runtime.NumGoroutine()
	*st = statusMap
	return nil
}

func (b *ByRoad) GetStatics(username string, statics *[]*StaticInfo) error {
	*statics = getStaticStatus()
	return nil
}
*/
