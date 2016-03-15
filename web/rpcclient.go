package web

import (
	"mysql_byroad/common"
	"net/rpc"
)

type RPCClient struct {
	protocol string
	Schema   string
	Desc     string
}

func NewRPCClient(protocol, schema, desc string) *RPCClient {
	client := RPCClient{
		protocol: protocol,
		Schema:   schema,
		Desc:     desc,
	}

	return &client
}

func (this *RPCClient) GetClient() (client *rpc.Client, err error) {
	client, err = rpc.DialHTTP(this.protocol, this.Schema)
	return
}

func (this *RPCClient) GetTask(taskid int64) (task *Task, err error) {
	client, err := this.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Close()
	err = client.Call("ByRoad.GetTask", taskid, &task)
	return
}

func (this *RPCClient) GetTasks(username string) (tasks []*Task, err error) {
	client, err := this.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Close()
	err = client.Call("ByRoad.GetTasks", username, &tasks)
	return
}

func (this *RPCClient) GetAllTasks(username string) (tasks []*Task, err error) {
	client, err := this.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Close()
	err = client.Call("ByRoad.GetAllTasks", username, &tasks)
	return
}

func (this *RPCClient) AddTask(task *Task) (status string, err error) {
	client, err := this.GetClient()
	if err != nil {
		return "", err
	}
	defer client.Close()
	err = client.Call("ByRoad.AddTask", task, &status)
	return
}

func (this *RPCClient) DeleteTask(taskid int64) (status string, err error) {
	client, err := this.GetClient()
	if err != nil {
		return "", err
	}
	defer client.Close()
	err = client.Call("ByRoad.DeleteTask", taskid, &status)
	return
}

func (this *RPCClient) UpdateTask(task *Task) (status string, err error) {
	client, err := this.GetClient()
	if err != nil {
		return "", err
	}
	defer client.Close()
	err = client.Call("ByRoad.UpdateTask", task, &status)
	return
}

func (this *RPCClient) ChangeTaskStat(task *Task) (status string, err error) {
	client, err := this.GetClient()
	if err != nil {
		return "", err
	}
	defer client.Close()
	err = client.Call("ByRoad.ChangeTaskStat", task, &status)
	return
}

func (this *RPCClient) GetColumns() (columns common.OrderedSchemas, err error) {
	client, err := this.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Close()
	err = client.Call("ByRoad.GetColumns", "", &columns)
	return
}

func (this *RPCClient) GetTaskColumns(task *Task) (columns map[string]map[string][]*NotifyField, err error) {
	client, err := this.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Close()
	err = client.Call("ByRoad.GetTaskColumns", task, &columns)
	return
}

/*
func (this *RPCClient) GetConfigMap() (configs []*Config, err error) {
	client, err := this.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Close()
	err = client.Call("ByRoad.GetConfigMap", "", &configs)
	return
}
*/
func (this *RPCClient) TaskExists(task *Task) (bool, error) {
	client, err := this.GetClient()
	if err != nil {
		return true, err
	}
	defer client.Close()
	var reply bool
	err = client.Call("ByRoad.TaskExists", task, &reply)
	return reply, err
}

func (this *RPCClient) TaskNameExists(name string) (bool, error) {
	client, err := this.GetClient()
	if err != nil {
		return true, err
	}
	defer client.Close()
	var reply bool
	err = client.Call("ByRoad.TaskNameExists", name, &reply)
	return reply, err
}

func (this *RPCClient) TasksQueueLen(tasks []*Task) (results [][]int64, err error) {
	client, err := this.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Close()
	err = client.Call("ByRoad.TasksQueueLen", tasks, &results)
	return
}

func (this *RPCClient) UpdateColumns() (columns common.OrderedSchemas, err error) {
	client, err := this.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Close()
	err = client.Call("ByRoad.UpdateColumns", "", &columns)
	return
}

func (this *RPCClient) GetStatus() (status map[string]interface{}, err error) {
	client, err := this.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Close()
	err = client.Call("ByRoad.GetStatus", "", &status)
	return
}

func (this *RPCClient) GetBinlogStatics() (statics *[]*BinlogStatic, err error) {
	client, err := this.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Close()
	err = client.Call("ByRoad.GetBinlogStatics", "", &statics)
	return
}

func (this *RPCClient) GetTaskStatic(taskid int64) (static *Static, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("ByRoad.GetTaskStatic", taskid, &static)
	return
}

func (this *RPCClient) GetTaskStatics() (static *TaskStatic, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("ByRoad.GetTaskStatics", 0, &static)
	return
}

func (this *RPCClient) GetLogList() (logList *LogList, err error) {
	client, err := this.GetClient()
	if err != nil {
		return
	}
	defer client.Close()
	err = client.Call("ByRoad.GetLogList", "", &logList)
	return
}
