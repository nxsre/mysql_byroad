package slave

import "reflect"
import "strings"

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

func isTableMatch(tab1, tab2 string) bool {
	return isMatch(tab1, tab2)
}

func isSchemaMatch(sch1, sch2 string) bool {
	return isMatch(sch1, sch2)
}

/*
如果s1有后缀*，则判断s2是否是能匹配
*/
func isMatch(s1, s2 string) bool {
	if s1 == s2 {
		return true
	}
	if strings.HasSuffix(s1, "*") && strings.HasPrefix(s2, s2[:len(s2)-1]) {
		return true
	}
	return false
}
