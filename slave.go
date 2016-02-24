// +build main

package main

import (
	"mysql-slave/slave"
)

func main() {
	slave.StartSlave()
}
