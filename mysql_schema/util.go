package schema

import (
	"strings"
)

func StringContains(s, subs string) bool {
	return strings.Contains(s, subs)
}

func StringContainsIgnoreCase(s, subs string) bool {
	s = strings.ToLower(s)
	subs = strings.ToLower(subs)
	return strings.Contains(s, subs)
}
