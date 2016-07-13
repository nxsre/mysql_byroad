package common

import "flag"

func ParseConfig() string {
	filename := flag.String("c", "config.toml", "config file path")
	flag.Parse()
	return *filename
}
