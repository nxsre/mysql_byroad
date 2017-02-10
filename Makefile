WORKPATH = $(PWD)
GOGET = go get -v
LDFLAGS = "-w -s -X main.buildstamp=`date '+%Y-%m-%d_%I:%M:%S'` -X main.githash=`git rev-parse HEAD`"
default: build
init-build-dir:
	mkdir -p build

byroad-dispatcher:
	cd ${WORKPATH}/dispatcher && go build -ldflags ${LDFLAGS}  -o ${WORKPATH}/build/byroad-dispatcher

byroad-monitor:
	cd ${WORKPATH}/monitor && go build -ldflags ${LDFLAGS} -o ${WORKPATH}/build/byroad-monitor

byroad-pusher:
	cd ${WORKPATH}/pusher && go build -ldflags ${LDFLAGS} -o ${WORKPATH}/build/byroad-pusher

nsq-monitor:
	cd ${WORKPATH}/nsq_monitor && go build -ldflags ${LDFLAGS} -o ${WORKPATH}/build/nsq_monitor

build:init-build-dir byroad-dispatcher byroad-monitor byroad-pusher nsq-monitor

build-dev:
	cd ${WORKPATH}/dispatcher && go build
	cd ${WORKPATH}/monitor && go build
	cd ${WORKPATH}/pusher && go build

clean:
	rm -rf *.log *.tar.gz build

tag:
	@gotags -R ${WORKPATH}/*.go > ${WORKPATH}/tags

todo:
	@grep --color=auto -r -n TODO ./*.go

tarsource:
	tar -czf byroad-src.tar.gz dispatcher model monitor nsq pusher vendor Makefile

copyweb:
	cd ${WORKPATH}/monitor && cp -R ${WORKPATH}/monitor/templates ${WORKPATH}/monitor/public ${WORKPATH}/build

tarbin:build copyweb
	tar -czf byroad-bin.tar.gz build


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
