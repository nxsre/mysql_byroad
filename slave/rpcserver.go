package slave

import (
	"encoding/json"
	"errors"
	"mysql_byroad/common"
	"mysql_byroad/model"
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
	Desc   string
}

type ByRoad struct {
	protocol string
	schema   string
	desc     string
}

func NewRPCServer(protocol, schema, desc string) *ByRoad {
	byroad := ByRoad{
		protocol: protocol,
		schema:   schema,
		desc:     desc,
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
		Desc:   this.desc,
	}
	message, err := json.Marshal(ss)
	if err != nil {
		return err
	}
	conn.Write(message)
	conn.Close()
	return nil
}

func (b *ByRoad) GetTask(taskid int64, task *model.Task) error {
	t := GetTask(taskid)
	if t == nil {
		return errors.New("task not found")
	}
	*task = *t
	return nil
}

func (b *ByRoad) GetTasks(username string, tasks *[]*model.Task) error {
	taskIdcmap.RLock()
	for _, t := range taskIdcmap.cmap {
		if username == t.CreateUser {
			t.Statistic = taskStatistics.Get(t.ID)
			*tasks = append(*tasks, t)
		}
	}
	taskIdcmap.RUnlock()
	sort.Sort(TaskSlice(*tasks))
	queueManager.TasksQueueLen(*tasks)
	return nil
}

func (b *ByRoad) GetAllTasks(username string, tasks *[]*model.Task) error {
	for _, t := range taskIdcmap.cmap {
		t.Statistic = taskStatistics.Get(t.ID)
		*tasks = append(*tasks, t)
	}
	sort.Sort(TaskSlice(*tasks))
	queueManager.TasksQueueLen(*tasks)
	return nil
}

func (b *ByRoad) AddTask(task *model.Task, status *string) error {
	id, err := task.Add()
	if err != nil {
		*status = "fail"
		sysLogger.Log(err.Error())
		owl.LogThisException(err.Error())
		return err
	}
	*status = "sucess"
	taskIdcmap.Set(id, task)
	if task.Stat == common.TASK_STATE_START {
		ntytasks.AddTask(task)
		routineManager.AddTaskRoutines(task)
	} else if task.Stat == common.TASK_STATE_STOP {
		routineManager.AddStopTaskRoutines(task)
	}
	return nil
}

func (b *ByRoad) DeleteTask(taskid int64, status *string) error {
	task := GetTask(taskid)
	if task == nil {
		return errors.New("not found")
	}
	taskIdcmap.Delete(task.ID)
	ntytasks.UpdateNotifyTaskMap(taskIdcmap)
	routineManager.StopTaskRoutine(task)
	err := deleteTask(task)
	if err != nil {
		*status = "fail"
		sysLogger.Log(err.Error())
		owl.LogThisException(err.Error())
		return err
	}
	*status = "success"
	return nil
}

func (b *ByRoad) UpdateTask(task *model.Task, status *string) error {
	taskIdcmap.Set(task.ID, task)
	err := task.Update()
	if err != nil {
		*status = "fail"
		sysLogger.Log(err.Error())
		owl.LogThisException(err.Error())
		return err
	}
	ntytasks.UpdateNotifyTaskMap(taskIdcmap)
	if task.Stat == common.TASK_STATE_START {
		routineManager.UpdateTaskRoutine(task)
	}
	*status = "success"
	return nil
}

func (b *ByRoad) ChangeTaskStat(task *model.Task, status *string) error {
	t := GetTask(task.ID)
	t.Stat = task.Stat
	err := t.SetStat()
	if err != nil {
		sysLogger.Log(err.Error())
		owl.LogThisException(err.Error())
		*status = "fail"
		return err
	}
	*status = "success"
	ntytasks.UpdateNotifyTaskMap(taskIdcmap)
	stat := task.Stat
	if stat == common.TASK_STATE_START {
		routineManager.StartTaskRoutine(task)
	} else if stat == common.TASK_STATE_STOP {
		routineManager.StopTaskRoutine(task)
	}
	return nil
}

func (b *ByRoad) GetColumns(username string, columns *model.OrderedSchemas) error {
	*columns = columnManager.GetOrderedColumns()
	return nil
}

func (b *ByRoad) GetTaskColumns(task *model.Task, columns *map[string]map[string]model.NotifyFields) error {
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
func (b *ByRoad) TaskExists(task *model.Task, reply *bool) error {
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

func (b *ByRoad) TasksQueueLen(tasks []*model.Task, results *[][]int64) error {
	*results = queueManager.TasksQueueLen(tasks)
	return nil
}

func (b *ByRoad) UpdateColumns(username string, columns *model.OrderedSchemas) error {
	columnManager.ReloadColumnsMap()
	*columns = columnManager.GetOrderedColumns()
	return nil
}

func (b *ByRoad) GetBinlogStatistics(username string, statistics *[]*model.BinlogStatistic) error {
	*statistics = binlogStatistics.Statistics
	return nil
}

func (b *ByRoad) GetStatus(username string, st *map[string]interface{}) error {
	start := startTime
	duration := time.Now().Sub(start)
	statusMap := make(map[string]interface{})
	statusMap["sendEventCount"] = totalStatistic.SendMessageCount
	statusMap["resendEventCount"] = totalStatistic.ReSendMessageCount
	statusMap["sendSuccessEventCount"] = totalStatistic.SendSuccessCount
	statusMap["sendFailedEventCount"] = totalStatistic.SendFailedCount
	statusMap["Start"] = start.String()
	statusMap["Duration"] = duration.String()
	statusMap["routineNumber"] = runtime.NumGoroutine()
	*st = statusMap
	return nil
}

func (b *ByRoad) GetTaskStatistic(taskid int64, statistic *model.Statistic) error {
	st := taskStatistics.Get(taskid)
	if st == nil {
		*statistic = *new(model.Statistic)
		return nil
	}
	*statistic = *st
	return nil
}

func (b *ByRoad) GetTaskStatistics(taskid int64, statistics *model.TaskStatistics) error {
	*statistics = *taskStatistics
	return nil
}

func (b *ByRoad) GetLogList(username string, logs *LogList) error {
	*logs = *(logList.GetLogList())
	return nil
}

func (b *ByRoad) GetMasterStatus(username string, binfo *BinlogInfo) error {
	*binfo = *GetMasterStatus()
	return nil
}

func (b *ByRoad) GetCurrentBinlogInfo(username string, binfo *BinlogInfo) error {
	*binfo = *binlogInfo
	return nil
}
