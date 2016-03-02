// +build main

package main

import (
	"mysql_byroad/slave"
)

func main() {
	slave.StartSlave()
}
