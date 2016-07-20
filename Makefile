WORKPATH = $(PWD)
GOGET = go get -v
default: build
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

byroad-dispatcher:
	cd ${WORKPATH}/dispatcher && godep go build -o ${WORKPATH}/build/byroad-dispatcher

byroad-monitor:
	cd ${WORKPATH}/monitor && godep go build -o ${WORKPATH}/build/byroad-monitor

byroad-pusher:
	cd ${WORKPATH}/pusher && godep go build -o ${WORKPATH}/build/byroad-pusher

build:init-build-dir byroad-dispatcher byroad-monitor byroad-pusher

dev:
	cd ${WORKPATH}/dispatcher && godep go build
	cd ${WORKPATH}/monitor && godep go build
	cd ${WORKPATH}/pusher && godep go build

clean:
	rm -rf *.log *.tar.gz build

tag:
	@gotags -R ${WORKPATH}/*.go > ${WORKPATH}/tags

todo:
	@grep --color=auto -r -n TODO ./*.go

tarsource:
	tar -czf byroad-src.tar.gz common dispatcher model monitor public pusher templates Makefile 

tarbin:build
	tar -czf byroad-bin.tar.gz build templates public


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
