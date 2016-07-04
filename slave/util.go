package slave

import "reflect"
import "strings"
import (
	"regexp"
	"mysql_byroad/model"
)

func isEqual(v1, v2 interface{}) bool {
	return reflect.DeepEqual(v1, v2)
}

/*
根据任务生成相应的推送redis队列的名字
*/
func genTaskQueueName(t *model.Task) string {
	return configer.GetRPCServer().Schema + t.Name
}

/*
根据任务生成相应的重推redis队列的名字
*/
func genTaskReQueueName(t *model.Task) string {
	return "re:" + configer.GetRPCServer().Schema + t.Name
}

func isTableMatch(tab1, tab2 string) bool {
	return isMatch(tab1, tab2)
}

func isSchemaMatch(sch1, sch2 string) bool {
	return isMatch(sch1, sch2)
}

/*
判断s2是否符合s1的规则
*/
func isMatch(s1, s2 string) bool {
	if s1 == s2 {
		return true
	}

	if strings.Index(s1, "*") != -1 {
		r, _ := regexp.Compile("^" + strings.Replace(s1, "*", "([\\w]+)", -1))
		if r.MatchString(s2) {
			return true
		}
	}

	return false
}
