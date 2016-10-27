package main

import (
	"mysql_byroad/model"
	"reflect"
	"regexp"

	log "github.com/Sirupsen/logrus"
)

func isEqual(v1, v2 interface{}) bool {
	return reflect.DeepEqual(v1, v2)
}

func isTableMatch(tab1, tab2 string) bool {
	return isMatch(tab1, tab2)
}

func isSchemaMatch(sch1, sch2 string) bool {
	return isMatch(sch1, sch2)
}

var patternSet map[string]*regexp.Regexp = make(map[string]*regexp.Regexp, 10)

/*
判断s2是否符合s1的规则
*/
func isMatch(s1, s2 string) bool {
	if s1 == s2 {
		return true
	}
	log.Debugf("s1: %s, s2: %s", s1, s2)
	var pat *regexp.Regexp
	var ok bool
	var err error
	if pat, ok = patternSet[s1]; !ok {
		pat, err = regexp.Compile("^" + s1 + "$")
		if err != nil {
			return false
		}
		patternSet[s1] = pat
	}
	return pat.MatchString(s2)
}

func genTaskQueueName(task *model.Task) string {
	return task.Name
}

func inStrs(strings []string, s string) bool {
	for _, str := range strings {
		if str == s {
			return true
		}
	}
	return false
}
