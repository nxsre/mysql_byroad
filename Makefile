WORKPATH = $(PWD)
GOGET = go get -v
default: run
backend_env:
	${GOGET} github.com/mattn/go-sqlite3
	${GOGET} github.com/go-sql-driver/mysql
	${GOGET} gopkg.in/macaron.v1
	${GOGET} github.com/go-macaron/pongo2
	${GOGET} github.com/jmoiron/sqlx
	${GOGET} github.com/go-macaron/session
	${GOGET} github.com/Unknwon/goconfig
	${GOGET} github.com/garyburd/redigo/redis
	${GOGET} github.com/go-macaron/binding
	${GOGET} github.com/siddontang/go-mysql/replication
	${GOGET} github.com/siddontang/go-mysql/client
	${GOGET} github.com/siddontang/go-mysql/mysql
	${GOGET} gopkg.in/redis.v2
	${GOGET} github.com/sadlil/gologger
	${GOGET} github.com/mattbaird/elastigo/lib
	${GOGET} github.com/mattn/go-isatty
	${GOGET} github.com/shiena/ansicolor

front_env:
	cd ${WORKPATH}/public && npm install

slaveserver:
	go build -tags "main" -o slave-main

webserver:
	go build -tags "web" -o slave-web

build:slaveserver webserver


run:
	${WORKPATH}/slave-web > slave-web.log 2>&1&
	${WORKPATH}/slave-main > slave-main.log 2>&1&

runmain:
	${WORKPATH}/slave-main > slave-main.log 2>&1&

runweb:
	${WORKPATH}/slave-web > slave-web.log 2>&1&

clean:
	@rm *.log *.tar.gz
tag:
	@gotags -R ${WORKPATH}/*.go > ${WORKPATH}/tags

todo:
	@grep --color=auto -r -n TODO ./*.go

tarsource:
	tar -czf slave-src.tar.gz *.go gorpool slave web common goticker templates public Makefile

tarbin:build
	tar -czf slave-bin.tar.gz slave-main slave-web templates public


