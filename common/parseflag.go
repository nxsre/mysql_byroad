package common

import "flag"

type Parser struct{}

func (this *Parser) ParseConfig() string {
	filename := flag.String("c", "config.conf", "config file path")
	flag.Parse()
	return *filename
}
