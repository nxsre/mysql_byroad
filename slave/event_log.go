package slave

import (
	"io/ioutil"
	"net/http"
)

type LogList struct {
	Logs []string
	Host string
	Path string
}

func NewLogList(host, path string) *LogList {
	return &LogList{
		Host: host,
		Path: path,
	}
}

func (this *LogList) Serve() {
	go http.ListenAndServe(this.Host, http.FileServer(http.Dir(this.Path)))
}

func (this *LogList) GetLogList() *LogList {
	loglist := this.getList()
	return &LogList{
		Logs: loglist,
		Host: this.Host,
		Path: this.Path,
	}
}

func (this *LogList) getList() []string {
	files := make([]string, 0, 15)
	names, err := ioutil.ReadDir(this.Path)
	if err != nil {
		sysLogger.LogErr(err)
		return files
	}
	for _, file := range names {
		if file.IsDir() {
			continue
		}
		files = append(files, file.Name())
	}
	return files
}
