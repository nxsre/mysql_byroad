package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync/atomic"
)

var count int64
var ch = make(chan (int))

func handler(res http.ResponseWriter, req *http.Request) {
	atomic.AddInt64(&count, 1)
	body, _ := ioutil.ReadAll(req.Body)
	fmt.Println(string(body))
	reses := []string{"success", "success"}
	choice := []int{0, 0, 0, 0, 0, 0, 0, 1}
	ret := reses[choice[rand.Intn(len(choice))]]
	//time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
	io.WriteString(res, ret)
}

func static(res http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(res, "count %d\n", count)
}

func server(port string) {
	fmt.Println("listening on " + port)
	http.ListenAndServe(":"+port, nil)
}

func main() {
	http.HandleFunc("/get", handler)
	http.HandleFunc("/", static)
	flag.Parse()
	port := flag.Arg(0)
	if port == "" {
		port = "8091"
	}
	go server(port)
	<-ch
}
