package slave

import (
	"encoding/json"
	"errors"
	"mysql-slave/common"
	"net"
	"net/http"
	"net/rpc"
	"runtime"
	"sort"
	"time"
)

type ServiceSignal struct {
	Code   string
	Schema string
}

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

func (this *ByRoad) start() {
	rpc.Register(this)
	rpc.HandleHTTP()
	l, e := net.Listen(this.protocol, this.schema)
	if e != nil {
		panic(e.Error())
	}
	go http.Serve(l, nil)
	this.register(configer.GetString("rpc", "schema"))

}

func (this *ByRoad) register(server string) error {
	return this.sendMessage(server, "0")
}

func (this *ByRoad) deregister(server string) error {
	return this.sendMessage(server, "1")
}

func (this *ByRoad) sendMessage(server, code string) error {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", server)
	if err != nil {
		return err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return err
	}
	ss := ServiceSignal{
		Code:   code,
		Schema: this.schema,
	}
	message, err := json.Marshal(ss)
	if err != nil {
		return err
	}
	conn.Write(message)
	conn.Close()
	return nil
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
			t.Static = taskStatic.Get(t.ID)
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
		t.Static = taskStatic.Get(t.ID)
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

func (b *ByRoad) UpdateColumns(username string, columns *common.OrderedSchemas) error {
	columnManager.ReloadColumnsMap()
	*columns = columnManager.GetOrderedColumns()
	return nil
}

func (b *ByRoad) GetBinlogStatics(username string, statics *[]*BinlogStatic) error {
	*statics = binlogStatics.Statics
	return nil
}

func (b *ByRoad) GetStatus(username string, st *map[string]interface{}) error {
	start := startTime
	duration := time.Now().Sub(start)
	statusMap := make(map[string]interface{})
	statusMap["sendEventCount"] = totalStatic.SendMessageCount
	statusMap["resendEventCount"] = totalStatic.ReSendMessageCount
	statusMap["sendSuccessEventCount"] = totalStatic.SendSuccessCount
	statusMap["sendFailedEventCount"] = totalStatic.SendFailedCount
	statusMap["Start"] = start.String()
	statusMap["Duration"] = duration.String()
	statusMap["routineNumber"] = runtime.NumGoroutine()
	*st = statusMap
	return nil
}

func (b *ByRoad) GetTaskStatic(taskid int64, static *Static) error {
	st := taskStatic.Get(taskid)
	if st == nil {
		*static = *new(Static)
		return nil
	}
	*static = *st
	return nil
}

func (b *ByRoad) GetTaskStatics(taskid int64, statics *TaskStatic) error {
	*statics = *taskStatic
	return nil
}
