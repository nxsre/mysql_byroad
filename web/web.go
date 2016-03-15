package web

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mysql_byroad/common"
	"net/http"
	"strings"
	"time"

	"os"
	"path/filepath"

	"github.com/go-macaron/binding"
	"github.com/go-macaron/pongo2"
	"github.com/go-macaron/session"
	_ "github.com/go-macaron/session/redis"
	"github.com/sadlil/gologger"
	"gopkg.in/macaron.v1"
)

type httpJsonResponse struct {
	Error   bool
	Message string
}

type UserInfo struct {
	UserName string `json:"username"`
	Mall     string `json:"mail"`
	FullName string `json:"fullname"`
}

type UserGroup struct {
	Groups []string `json:"groups"`
}

type TaskForm struct {
	Name           string `form:"name" binding:"AlphaDash;MaxSize(50);Required"`
	Apiurl         string `form:"apiurl" binding:"Required"`
	RoutineCount   int    `form:"routineCount" binding:"Range(1,100)"`
	ReRoutineCount int    `form:"reRoutineCount" binding:"Range(1,100)"`
	ReSendTime     int    `form:"reSendTime" binding:"Range(0,30000)"`
	RetryCount     int    `form:"retryCount" binding:"Range(0,10)"`
	Timeout        int    `form:"timeout" binding:"Range(1,30000)"`
	Desc           string `form:"desc" binding:"MaxSize(255)"`
	State          string `form:"state"`
}

var rpcManager *RPCClientManager
var ac *common.AuthConfig
var logger gologger.GoLogger
var configFile string
var configer *common.Configer

func StartServer() {
	var err error
	logger = gologger.GetLogger(gologger.FILE, "slave-web.log")
	parser := new(common.Parser)
	configFile = parser.ParseConfig()
	configer, err = common.NewConfiger(configFile)
	if err != nil {
		panic(err.Error())
	}
	rpcManager = NewRPCClientManager()
	rpcManager.HandleSignal(configer.GetString("rpc", "schema"))
	ac = configer.GetAuth()
	wc := configer.GetWeb()
	os.Setenv("HOST", wc.Host)
	os.Setenv("PORT", wc.Port)
	m := macaron.New()
	m.Use(macaron.Recovery())
	m.Use(macaron.Static("public",
		macaron.StaticOptions{
			SkipLogging: true,
		},
	))

	m.Use(pongo2.Pongoer())
	m.Use(session.Sessioner(session.Options{
		CookiePath:  "/",
		Gclifetime:  3600,
		Maxlifetime: 3600,
		//Provider:       "redis",
		//ProviderConfig: fmt.Sprintf("addr=%s:%s", redisHost, redisPort),
	}))
	m.Use(func(ctx *macaron.Context, sess session.Store) {
		if ctx.Req.URL.Path == "/auth/login" {
			return
		}
		username := sess.Get("user")
		if username == nil {
			ctx.Redirect(fmt.Sprintf("%s/api/login/?camefrom=%s", ac.AuthUrl, ac.AppName))
		} else {
			if checkAuth(ctx, sess, "admin") {
				ctx.Data["isAdmin"] = true
			}
			ctx.Data["username"] = username.(string)
			clients := rpcManager.GetClients()
			ctx.Data["clients"] = clients
		}
	})

	m.Get("/auth/login", login)
	m.Get("/auth/logout", logout)
	m.Get("/", tasklist)
	m.Get("/status", status)
	m.Get("/addtask", addTaskHTML)
	m.Get("/task", tasklist)
	m.Get("/taskmodify/:taskid", modifytask)
	m.Get("/log", loglist)
	m.Get("/log/download/:filename", downloadlog)

	m.Post("/task", binding.Bind(TaskForm{}), doAddTask)
	m.Post("/task/changeStat/:taskid", changeTaskStat)

	m.Put("/task", binding.Bind(TaskForm{}), doUpdateTask)

	m.Delete("/task/:taskid", doDeleteTask)

	m.Get("/column/update", updateColumnMap)
	m.Get("/taskdetail/:taskid", getTaskStatic)
	m.Run()
}

func getUsername(sess session.Store) string {
	username := sess.Get("user")
	if username == nil {
		return ""
	}
	return username.(string)
}

//判断用户是否拥有flag的权限
func checkAuth(ctx *macaron.Context, sess session.Store, flag string) bool {
	groups := sess.Get("groups").(string)
	isAdmin := strings.Index(groups, "admin")
	if isAdmin != -1 {
		return true
	}
	index := strings.Index(groups, flag)
	if index != -1 {
		return true
	}
	return false
}

//判断任务是否属于该用户
func checkTaskUser(t *Task, sess session.Store) bool {
	groups := sess.Get("groups").(string)
	isAdmin := strings.Index(groups, "admin")
	if isAdmin != -1 {
		return true
	}
	user := sess.Get("user").(string)
	return user == t.CreateUser
	return true
}

func return403(ctx *macaron.Context) string {
	resp := new(httpJsonResponse)
	ctx.Resp.WriteHeader(403)
	resp.Error = true
	resp.Message = "权限不够"
	body, _ := json.Marshal(resp)
	return string(body)
}

func return404(ctx *macaron.Context) string {
	resp := new(httpJsonResponse)
	ctx.Resp.WriteHeader(404)
	resp.Error = true
	resp.Message = "未找到"
	body, _ := json.Marshal(resp)
	return string(body)
}

func login(ctx *macaron.Context, sess session.Store) string {
	token := ctx.Query("token")
	username := ctx.Query("username")
	t := sha1.New()
	io.WriteString(t, fmt.Sprintf("%s%s%s", token, ac.AppKey, username))
	sessionId := fmt.Sprintf("%x", t.Sum(nil))

	resp, err := http.Get(fmt.Sprintf("%s/api/info/?session_id=%s", ac.AuthUrl, sessionId))
	if err != nil {
		return "api请求错误.#1"
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "api请求错误.#2"
	}

	var user UserInfo
	err = json.Unmarshal(body, &user)
	if err != nil {
		return err.Error()
	}

	if user.FullName != "" {
	} else {
		return "登录失败.#3"
	}

	apiUrl := fmt.Sprintf("%s/api/grouprole/?uid=%s&app_key=%s&app_name=%s", ac.AuthUrl, username, ac.AppKey, ac.AppName)

	resp, err = http.Get(apiUrl)
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return "api请求错误.#4"
	}
	defer resp.Body.Close()
	var groups UserGroup
	err = json.Unmarshal(body, &groups)
	if err != nil {
		return "api请求错误.#5"
	}

	if len(groups.Groups) == 0 {
		return "你没有旁路平台权限."
	}
	sess.Set("username", user.FullName)
	sess.Set("groups", array2string(groups.Groups))
	sess.Set("user", user.UserName)

	ctx.Redirect("/")
	logger.Info(fmt.Sprintf("%s login", user.UserName))

	return "登陆成功."
}

func array2string(arr []string) string {
	var str string
	for _, s := range arr {
		str = str + s + ","
	}
	str = strings.TrimRight(str, ",")
	return str
}

func logout(ctx *macaron.Context, sess session.Store) {
	sess.Destory(ctx)
	ctx.Redirect(fmt.Sprintf("%s/api/login/?camefrom=%s", ac.AppName))
}

func index(ctx *macaron.Context, sess session.Store) {
	if !checkAuth(ctx, sess, "all") {
		ctx.HTML(403, "403")
		return
	}
	ctx.HTML(200, "index")
}

func status(ctx *macaron.Context, sess session.Store) {
	if !checkAuth(ctx, sess, "admin") {
		ctx.HTML(403, "403")
		return
	}
	rpcclient := rpcManager.GetClient(ctx.GetCookie("client"))
	if rpcclient != nil {
		st, _ := rpcclient.GetStatus()
		ctx.Data["sendEventCount"] = st["sendEventCount"]
		ctx.Data["resendEventCount"] = st["resendEventCount"]
		ctx.Data["sendSuccessEventCount"] = st["sendSuccessEventCount"]
		ctx.Data["sendFailedEventCount"] = st["sendFailedEventCount"]
		ctx.Data["Start"] = st["Start"]
		ctx.Data["Duration"] = st["Duration"]
		ctx.Data["routineNumber"] = st["routineNumber"]
		status, _ := rpcclient.GetBinlogStatics()
		ctx.Data["Status"] = status
	}
	ctx.HTML(200, "status")
}

func addTaskHTML(ctx *macaron.Context, sess session.Store) {
	if !checkAuth(ctx, sess, "all") {
		ctx.HTML(403, "403")
		return
	}
	rpcclient := rpcManager.GetClient(ctx.GetCookie("client"))
	if rpcclient != nil {
		colslist, _ := rpcclient.GetColumns()
		ctx.Data["colslist"] = colslist
	}
	ctx.HTML(200, "addtask")
}

func modifytask(ctx *macaron.Context, sess session.Store) {
	if !checkAuth(ctx, sess, "all") {
		ctx.HTML(403, "403")
		return
	}
	rpcclient := rpcManager.GetClient(ctx.GetCookie("client"))
	if rpcclient != nil {
		taskid := ctx.ParamsInt64("taskid")
		task, _ := rpcclient.GetTask(taskid)
		if task == nil {
			ctx.HTML(404, "404")
			return
		}
		if !checkTaskUser(task, sess) {
			ctx.HTML(403, "403")
			return
		}
		ctx.Data["task"] = task
		taskColumnsMap, _ := rpcclient.GetTaskColumns(task)
		ctx.Data["taskColumnsMap"] = taskColumnsMap
		colslist, _ := rpcclient.GetColumns()
		ctx.Data["colslist"] = colslist
	}

	ctx.HTML(200, "modifytask")
}

func doAddTask(t TaskForm, ctx *macaron.Context, sess session.Store) string {
	fields := ctx.QueryStrings("fields")
	if !checkAuth(ctx, sess, "all") {
		return return403(ctx)
	}
	resp := new(httpJsonResponse)
	resp.Error = false
	if len(fields) == 0 {
		resp.Error = true
		resp.Message = "请添加字段信息"
		body, _ := json.Marshal(resp)
		return string(body)
	}
	rpcclient := rpcManager.GetClient(ctx.GetCookie("client"))
	if rpcclient == nil {
		return "RPC client not exists!"
	}

	exists, _ := rpcclient.TaskNameExists(t.Name)
	if exists {
		resp.Error = true
		resp.Message = "名字已存在"
		body, _ := json.Marshal(resp)
		return string(body)
	}
	task := new(Task)
	copyTask(&t, task)
	task.CreateUser = sess.Get("user").(string)
	task.Fields = make([]*NotifyField, 0)
	for _, c := range fields {
		send := ctx.QueryInt(c)
		f := new(NotifyField)
		f.Send = send
		nfs := strings.Split(c, ".")
		if len(nfs) < 3 {
			resp.Error = true
			resp.Message = "参数错误"
			body, _ := json.Marshal(resp)
			return string(body)
		}
		f.Schema = nfs[0]
		f.Table = nfs[1]
		f.Column = nfs[2]
		if !FieldExists(task, f) {
			task.Fields = append(task.Fields, f)
		}
	}

	_, err := rpcclient.AddTask(task)
	if err != nil {
		resp.Error = true
		resp.Message = "添加失败!"
	} else {
		resp.Message = "添加成功!"
	}
	body, _ := json.Marshal(resp)
	ctx.Resp.WriteHeader(201)

	logger.Info(fmt.Sprintf("%s %s name=%s", task.CreateUser, "Add", task.Name))
	return string(body)
}

func doDeleteTask(ctx *macaron.Context, sess session.Store) string {
	id := ctx.ParamsInt64("taskid")
	rpcclient := rpcManager.GetClient(ctx.GetCookie("client"))
	if rpcclient == nil {
		return "RPC client not exists!"
	}
	task, _ := rpcclient.GetTask(id)
	if task == nil {
		return return404(ctx)
	}
	if !checkAuth(ctx, sess, "all") || !checkTaskUser(task, sess) {
		return return403(ctx)
	}
	resp := new(httpJsonResponse)
	resp.Error = false
	_, err := rpcclient.DeleteTask(id)
	if err != nil {
		resp.Error = true
		resp.Message = "删除失败"
	} else {
		resp.Message = "删除成功"
	}
	body, _ := json.Marshal(resp)
	ctx.Resp.WriteHeader(204)
	logger.Info(fmt.Sprintf("%s %s name=%s", task.CreateUser, "Delete", task.Name))
	return string(body)
}

func doUpdateTask(t TaskForm, ctx *macaron.Context, sess session.Store) string {
	taskid := ctx.QueryInt64("taskid")
	fields := ctx.QueryStrings("fields")
	resp := new(httpJsonResponse)
	resp.Error = false
	if len(fields) == 0 {
		resp.Error = true
		resp.Message = "请添加字段信息"
		body, _ := json.Marshal(resp)
		return string(body)
	}
	rpcclient := rpcManager.GetClient(ctx.GetCookie("client"))
	if rpcclient == nil {
		return "RPC client not exists!"
	}
	task, _ := rpcclient.GetTask(taskid)
	if task == nil {
		return return404(ctx)
	}
	if !checkAuth(ctx, sess, "all") || !checkTaskUser(task, sess) {
		return return403(ctx)
	}
	if task.Name != t.Name {
		resp.Error = true
		resp.Message = "名字不能修改"
		body, _ := json.Marshal(resp)
		return string(body)
	}
	copyTask(&t, task)
	task.Fields = make([]*NotifyField, 0)
	for _, c := range fields {
		send := ctx.QueryInt(c)
		f := new(NotifyField)
		f.Send = send
		nfs := strings.Split(c, ".")
		if len(nfs) < 3 {
			resp.Error = true
			resp.Message = "参数错误"
			body, _ := json.Marshal(resp)
			return string(body)
		}
		f.Schema = nfs[0]
		f.Table = nfs[1]
		f.Column = nfs[2]
		if !FieldExists(task, f) {
			task.Fields = append(task.Fields, f)
		}
	}
	_, err := rpcclient.UpdateTask(task)
	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
	} else {
		resp.Message = "更新成功!"
	}
	body, _ := json.Marshal(resp)
	ctx.Resp.WriteHeader(201)
	logger.Info(fmt.Sprintf("%s %s name=%s", task.CreateUser, "Update", task.Name))
	return string(body)
}

func tasklist(ctx *macaron.Context, sess session.Store) {
	if !checkAuth(ctx, sess, "all") {
		ctx.HTML(403, "403")
		return
	}
	rpcclient := rpcManager.GetClient(ctx.GetCookie("client"))
	if rpcclient != nil {
		var sortTasks []*Task
		if checkAuth(ctx, sess, "admin") {
			sortTasks, _ = rpcclient.GetAllTasks(sess.Get("user").(string))
		} else {
			sortTasks, _ = rpcclient.GetTasks(sess.Get("user").(string))
		}
		ctx.Data["tasks"] = sortTasks
	}

	ctx.HTML(200, "tasklist")
}

func changeTaskStat(ctx *macaron.Context, sess session.Store) string {
	taskid := ctx.ParamsInt64("taskid")
	rpcclient := rpcManager.GetClient(ctx.GetCookie("client"))
	if rpcclient == nil {
		return "RPC client not exists!"
	}
	task, _ := rpcclient.GetTask(taskid)
	if task == nil {
		return return404(ctx)
	}
	if !checkAuth(ctx, sess, "all") || !checkTaskUser(task, sess) {
		return return403(ctx)
	}
	stat := ctx.Query("stat")
	task.Stat = stat
	_, err := rpcclient.ChangeTaskStat(task)
	resp := new(httpJsonResponse)
	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
	} else {
		resp.Error = false
		resp.Message = "操作成功"
	}
	body, _ := json.Marshal(resp)
	logger.Info(fmt.Sprintf("%s %s name=%s", task.CreateUser, "change state", task.Name))
	return string(body)
}

func loglist(ctx *macaron.Context, sess session.Store) {
	if !checkAuth(ctx, sess, "admin") {
		ctx.HTML(403, "403")
		return
	}
	rpcclient := rpcManager.GetClient(ctx.GetCookie("client"))
	if rpcclient != nil {
		logList, _ := rpcclient.GetLogList()
		ctx.Data["loglist"] = logList.Logs
		ctx.Data["Host"] = logList.Host
	}
	ctx.HTML(200, "loglist")
}

func downloadlog(ctx *macaron.Context, sess session.Store) {
	if !checkAuth(ctx, sess, "admin") {
		ctx.HTML(403, "403")
		return
	}
	filename := ctx.Params("filename")
	rpcclient := rpcManager.GetClient(ctx.GetCookie("client"))
	if rpcclient != nil {
		logList, _ := rpcclient.GetLogList()
		host := logList.Host
		resp, err := http.Get("http://" + host + "/" + filename)
		if err != nil {
			logger.Error(err.Error())
			ctx.Write([]byte(err.Error()))
			return
		}
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			logger.Error(err.Error())
			ctx.Write([]byte(err.Error()))
			return
		}
		ctx.ServeContent(filename, bytes.NewReader(b))
	}
}

func updateColumnMap(ctx *macaron.Context, sess session.Store) {
	if !checkAuth(ctx, sess, "all") {
		return
	}
	rpcclient := rpcManager.GetClient(ctx.GetCookie("client"))
	if rpcclient != nil {
		colslist, _ := rpcclient.UpdateColumns()
		ctx.Data["colslist"] = colslist
	}
	ctx.HTML(200, "columnlist")
}

func copyTask(src *TaskForm, dst *Task) {
	dst.Name = strings.TrimSpace(src.Name)
	dst.Apiurl = strings.TrimSpace(src.Apiurl)
	dst.RoutineCount = src.RoutineCount
	dst.ReRoutineCount = src.ReRoutineCount
	dst.ReSendTime = src.ReSendTime
	dst.RetryCount = src.RetryCount
	dst.Timeout = src.Timeout
	dst.CreateTime = time.Now()
	dst.Desc = strings.TrimSpace(src.Desc)
	dst.Stat = src.State
}

func FieldExists(task *Task, field *NotifyField) bool {
	for _, f := range task.Fields {
		if f.Schema == field.Schema && f.Table == field.Table && f.Column == field.Column {
			return true
		}
	}
	return false
}

func getLogList(client string) ([]string, error) {
	files := make([]string, 0, 15)
	names, err := ioutil.ReadDir(filepath.Join(configer.GetString("web", "logdir", "log/"), client))
	if err != nil {
		logger.Error(fmt.Sprintf(err.Error()))
		return nil, err
	}
	for _, file := range names {
		if file.IsDir() {
			continue
		}
		files = append(files, file.Name())
	}
	return files, nil
}

func getTaskStatic(ctx *macaron.Context, sess session.Store) {
	taskid := ctx.ParamsInt64("taskid")
	if !checkAuth(ctx, sess, "all") {
		ctx.HTML(403, "403")
		return
	}
	rpcclient := rpcManager.GetClient(ctx.GetCookie("client"))
	if rpcclient == nil {
		ctx.HTML(404, "404")
		return
	}
	static, _ := rpcclient.GetTaskStatic(taskid)
	ctx.Data["static"] = static
	ctx.HTML(200, "taskdetail")
}
