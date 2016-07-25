package main

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mysql_byroad/model"
	"net/http"
	"sort"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/go-macaron/binding"
	"github.com/go-macaron/pongo2"
	"github.com/go-macaron/session"
	"gopkg.in/macaron.v1"
)

type httpJsonResponse struct {
	Error   bool
	Message string
}

type UserInfo struct {
	UserName string `json:"username"`
	Mail     string `json:"mail"`
	FullName string `json:"fullname"`
}

type UserGroup struct {
	Groups []string `json:"groups"`
}

type TaskForm struct {
	Name           string                 `form:"name" binding:"AlphaDash;MaxSize(50);Required"`
	Apiurl         string                 `form:"apiurl" binding:"Required"`
	RoutineCount   int                    `form:"routineCount" binding:"Range(1,10)"`
	ReRoutineCount int                    `form:"reRoutineCount" binding:"Range(1,10)"`
	ReSendTime     int                    `form:"reSendTime" binding:"Range(0,30000)"`
	RetryCount     int                    `form:"retryCount" binding:"Range(0,10)"`
	Timeout        int                    `form:"timeout" binding:"Range(1,30000)"`
	Desc           string                 `form:"desc" binding:"MaxSize(255)"`
	State          string                 `form:"state"`
	PackProtocal   model.DataPackProtocal `form:"packProtocal"`
}

func StartServer() {
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
		if !Conf.Debug {
			if username == nil {
				ctx.Redirect(fmt.Sprintf("%s/api/login/?camefrom=%s", Conf.WebConfig.AuthURL, Conf.WebConfig.AppName))
			} else {
				if checkAuth(ctx, sess, "admin") {
					ctx.Data["isAdmin"] = true
				}
				ctx.Data["username"] = username.(string)
				ctx.Data["clients"] = dispatcherManager.GetRPCClients()
			}
		} else {
			sess.Set("user", "test")
			ctx.Data["isAdmin"] = true
			ctx.Data["username"] = "test"
			ctx.Data["clients"] = dispatcherManager.GetRPCClients()
		}
	})

	m.Get("/auth/login", login)
	m.Get("/auth/logout", logout)
	m.Get("/", tasklist)
	m.Get("/status", status)
	m.Get("/addtask", addTaskHTML)
	m.Get("/task", tasklist)
	m.Get("/taskmodify/:taskid", modifytask)

	m.Post("/task", binding.Bind(TaskForm{}), doAddTask)
	m.Post("/task/changeStat/:taskid", changeTaskStat)

	m.Put("/task", binding.Bind(TaskForm{}), doUpdateTask)

	m.Delete("/task/:taskid", doDeleteTask)

	m.Get("/task/detail/:taskid", getTaskStatistic)
	m.Run(Conf.WebConfig.Host, Conf.WebConfig.Port)
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
	if Conf.Debug {
		return true
	}
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
func checkTaskUser(t *model.Task, sess session.Store) bool {
	if Conf.Debug {
		return true
	}
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
	io.WriteString(t, fmt.Sprintf("%s%s%s", token, Conf.WebConfig.AppKey, username))
	sessionId := fmt.Sprintf("%x", t.Sum(nil))

	resp, err := http.Get(fmt.Sprintf("%s/api/info/?session_id=%s", Conf.WebConfig.AuthURL, sessionId))
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

	apiUrl := fmt.Sprintf("%s/api/grouprole/?uid=%s&app_key=%s&app_name=%s", Conf.WebConfig.AuthURL, username, Conf.WebConfig.AppKey, Conf.WebConfig.AppName)

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
	ctx.Redirect(fmt.Sprintf("%s/api/login/?camefrom=%s", Conf.WebConfig.AppName))
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
	client := ctx.GetCookie("client")
	if rpcclient, ok := dispatcherManager.GetRPCClient(client); ok {
		status, _ := rpcclient.GetBinlogStatistics()
		masterStatus, _ := rpcclient.GetMasterStatus()
		currentBinlogInfo, _ := rpcclient.GetCurrentBinlogInfo()
		st, _ := rpcclient.GetSysStatus()
		ctx.Data["Status"] = status
		ctx.Data["MasterStatus"] = masterStatus
		ctx.Data["CurrentBinlogInfo"] = currentBinlogInfo
		ctx.Data["Start"] = st["Start"]
		ctx.Data["Duration"] = st["Duration"]
		ctx.Data["routineNumber"] = st["routineNumber"]
	}
	ctx.HTML(200, "status")
}

func addTaskHTML(ctx *macaron.Context, sess session.Store) {
	if !checkAuth(ctx, sess, "all") {
		ctx.HTML(403, "403")
		return
	}
	client := ctx.GetCookie("client")
	colslist, _ := dispatcherManager.GetColumns(client)
	ctx.Data["colslist"] = colslist

	ctx.HTML(200, "addtask")
}

func modifytask(ctx *macaron.Context, sess session.Store) {
	if !checkAuth(ctx, sess, "all") {
		ctx.HTML(403, "403")
		return
	}
	taskid := ctx.ParamsInt64("taskid")
	task := &model.Task{
		ID: taskid,
	}
	ex, _ := task.Exists()
	if !ex {
		ctx.HTML(404, "404")
		return
	}
	if !checkTaskUser(task, sess) {
		ctx.HTML(403, "403")
		return
	}
	ctx.Data["task"] = task
	ctx.Data["taskColumnsMap"] = task.GetTaskColumnsMap()
	client := ctx.GetCookie("client")
	colslist, _ := dispatcherManager.GetColumns(client)
	ctx.Data["colslist"] = colslist

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
	task := new(model.Task)
	copyTask(&t, task)
	task.CreateUser = sess.Get("user").(string)
	task.Fields = *new(model.NotifyFields)
	for _, c := range fields {
		send := ctx.QueryInt(c)
		f := new(model.NotifyField)
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
	if ex, _ := task.NameExists(); ex {
		resp.Error = true
		resp.Message = "任务名已经存在!"
		body, _ := json.Marshal(resp)
		return string(body)
	}

	_, err := task.Add()
	if err != nil {
		resp.Error = true
		resp.Message = "添加失败!"
	} else {
		resp.Message = "添加成功!"
	}
	dispatcherManager.AddTask(task)
	if err != nil {
		log.Error("add task error: ", err.Error())
	}
	pusherManager.AddTask(task)
	body, _ := json.Marshal(resp)
	ctx.Resp.WriteHeader(201)

	return string(body)
}

func doDeleteTask(ctx *macaron.Context, sess session.Store) string {
	id := ctx.ParamsInt64("taskid")
	if !checkAuth(ctx, sess, "all") {
		return return403(ctx)
	}
	resp := new(httpJsonResponse)
	resp.Error = false
	task := &model.Task{
		ID: id,
	}
	if ext, _ := task.Exists(); !ext {
		return return403(ctx)
	}
	if !checkTaskUser(task, sess) {
		return return403(ctx)
	}
	err := task.Delete()
	if err != nil {
		resp.Error = true
		resp.Message = "删除失败"
	} else {
		resp.Message = "删除成功"
	}
	dispatcherManager.DeleteTask(task)
	pusherManager.DeleteTask(task)
	body, _ := json.Marshal(resp)
	ctx.Resp.WriteHeader(204)
	return string(body)
}

func doUpdateTask(t TaskForm, ctx *macaron.Context, sess session.Store) string {
	taskid := ctx.QueryInt64("taskid")
	if !checkAuth(ctx, sess, "all") {
		return return403(ctx)
	}
	resp := new(httpJsonResponse)
	resp.Error = false
	task := &model.Task{
		ID: taskid,
	}
	if ext, _ := task.Exists(); !ext {
		return return403(ctx)
	}
	if !checkTaskUser(task, sess) {
		return return403(ctx)
	}
	fields := ctx.QueryStrings("fields")
	if len(fields) == 0 {
		resp.Error = true
		resp.Message = "请添加字段信息"
		body, _ := json.Marshal(resp)
		return string(body)
	}
	if task.Name != t.Name {
		resp.Error = true
		resp.Message = "名字不能修改"
		body, _ := json.Marshal(resp)
		return string(body)
	}
	copyTask(&t, task)
	task.Fields = *new(model.NotifyFields)
	for _, c := range fields {
		send := ctx.QueryInt(c)
		f := new(model.NotifyField)
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

	err := task.Update()
	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
	} else {
		resp.Message = "更新成功!"
	}
	dispatcherManager.UpdateTask(task)
	pusherManager.UpdateTask(task)
	body, _ := json.Marshal(resp)
	ctx.Resp.WriteHeader(201)
	return string(body)
}

func tasklist(ctx *macaron.Context, sess session.Store) {
	if !checkAuth(ctx, sess, "all") {
		ctx.HTML(403, "403")
		return
	}
	var sortTasks []*model.Task
	if checkAuth(ctx, sess, "admin") {
		sortTasks, _ = model.GetAllTask()
	} else {
		sortTasks, _ = model.GetTasks(sess.Get("user").(string))
	}
	sort.Sort(TaskSlice(sortTasks))
	ctx.Data["tasks"] = sortTasks
	ctx.HTML(200, "tasklist")
}

func changeTaskStat(ctx *macaron.Context, sess session.Store) string {
	taskid := ctx.ParamsInt64("taskid")
	if !checkAuth(ctx, sess, "all") {
		return return403(ctx)
	}
	resp := new(httpJsonResponse)
	resp.Error = false
	task := &model.Task{
		ID: taskid,
	}
	if ext, _ := task.Exists(); !ext {
		return return403(ctx)
	}
	if !checkTaskUser(task, sess) {
		return return403(ctx)
	}
	stat := ctx.Query("stat")
	task.Stat = stat
	err := task.SetStat()
	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
	} else {
		resp.Error = false
		resp.Message = "操作成功"
	}
	if stat == model.TASK_STATE_START {
		nsqManager.UnPauseTopic(task.Name)
	} else if stat == model.TASK_STATE_STOP {
		nsqManager.PauseTopic(task.Name)
	}
	dispatcherManager.UpdateTask(task)
	pusherManager.UpdateTask(task)
	body, _ := json.Marshal(resp)
	return string(body)
}

func copyTask(src *TaskForm, dst *model.Task) {
	dst.Name = strings.TrimSpace(src.Name)
	dst.Apiurl = strings.TrimSpace(src.Apiurl)
	dst.RoutineCount = src.RoutineCount
	dst.ReRoutineCount = src.ReRoutineCount
	dst.PackProtocal = src.PackProtocal
	dst.ReSendTime = src.ReSendTime
	dst.RetryCount = src.RetryCount
	dst.Timeout = src.Timeout
	dst.CreateTime = time.Now()
	dst.Desc = strings.TrimSpace(src.Desc)
	dst.Stat = src.State
	dst.PackProtocal = src.PackProtocal
}

func FieldExists(task *model.Task, field *model.NotifyField) bool {
	for _, f := range task.Fields {
		if f.Schema == field.Schema && f.Table == field.Table && f.Column == field.Column {
			return true
		}
	}
	return false
}

func getTaskStatistic(ctx *macaron.Context, sess session.Store) {
	taskid := ctx.ParamsInt64("taskid")
	if !checkAuth(ctx, sess, "all") {
		ctx.HTML(403, "403")
		return
	}
	task := &model.Task{
		ID: taskid,
	}
	if ext, _ := task.Exists(); !ext {
		ctx.HTML(403, "403")
		return
	}
	if !checkTaskUser(task, sess) {
		ctx.HTML(403, "403")
		return
	}
	stats := nsqManager.GetTopicStats(task.Name)
	ctx.Data["statistics"] = stats
	ctx.HTML(200, "taskdetail")
}
