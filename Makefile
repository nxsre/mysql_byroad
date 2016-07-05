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
init-build-dir:
	mkdir -p build
slaveserver:
	cd slave/bin &&  go build -tags "main" -o ${WORKPATH}/build/slave-main

webserver:
	cd web/bin/ && go build -tags "web" -o ${WORKPATH}/build/slave-web

build:init-build-dir slaveserver webserver


run:
	${WORKPATH}/build/slave-web > ${WORKPATH}/build/slave-web.log 2>&1&
	${WORKPATH}/build/slave-main > ${WORKPATH}/build/slave-main.log 2>&1&

runmain:
	${WORKPATH}/build/slave-main > ${WORKPATH}/build/slave-main.log 2>&1&

runweb:
	${WORKPATH}/build/slave-web > ${WORKPATH}/build/slave-web.log 2>&1&

clean:
	rm -rf *.log *.tar.gz build
tag:
	@gotags -R ${WORKPATH}/*.go > ${WORKPATH}/tags

todo:
	@grep --color=auto -r -n TODO ./*.go

tarsource:
	tar -czf slave-src.tar.gz common gorpool goticker model public slave templates web Makefile

tarbin:build
	tar -czf slave-bin.tar.gz build/slave-main build/slave-web templates public


savedep:
	hg rm  ${WORKPATH}/slave/bin/Godeps
	hg rm  ${WORKPATH}/web/bin/Godeps
	hg ci -m "romove dep defs"
	cd web/bin/ && godep save 
	cd ${WORKPATH}/slave/bin/  &&  godep save
	hg add ${WORKPATH}/slave/bin/Godeps
	hg add  ${WORKPATH}/web/bin/Godeps
	hg ci -m "update dep defs"

restoredep:
	cd web/bin/ && godep restore
	cd ${WORKPATH}/slave/bin/ && godep restore
