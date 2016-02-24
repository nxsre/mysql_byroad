package slave

import "reflect"

func isEqual(v1, v2 interface{}) bool {
	return reflect.DeepEqual(v1, v2)
}

/*
根据任务生成相应的推送redis队列的名字
*/
func genTaskQueueName(t *Task) string {
	return configer.GetRPCServer().Schema + t.Name
}

/*
根据任务生成相应的重推redis队列的名字
*/
func genTaskReQueueName(t *Task) string {
	return "re:" + configer.GetRPCServer().Schema + t.Name
}
