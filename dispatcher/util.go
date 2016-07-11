package main

import (
	"mysql_byroad/model"
	"reflect"
)
import "strings"
import "regexp"

func isEqual(v1, v2 interface{}) bool {
	return reflect.DeepEqual(v1, v2)
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

func genTaskQueueName(task *model.Task) string {
	return task.Name
}
