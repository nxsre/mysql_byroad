package main

import (
	"crypto/sha1"
	"crypto/tls"
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
	"github.com/go-macaron/gzip"
	"github.com/go-macaron/pongo2"
	"github.com/go-macaron/session"
	"gopkg.in/macaron.v1"
)

type httpJsonResponse struct {
	Error   bool
	Message string
}

// {"username": "xiny1", "mail": "xiny1@jumei.com", "fullname": "\u6768\u946b"}
type UserInfo struct {
	UserName string `json:"username"`
	Mail     string `json:"mail"`
	FullName string `json:"fullname"`
}

/*
group role: {
  "all_warehouse": false,
  "paths": [],
  "categorys": [],
  "all_category": false,
  "all_sku_category": false,
  "md5_change": true,
  "groups": [
    "admin"
  ],
  "brands": [],
  "all_brand": false,
  "warehouses": [],
  "skus": []
}
*/
type UserGroup struct {
	Groups []string `json:"groups"`
}

type FieldsForm struct {
	Schema string `json:"schema"`
	Tables []struct {
		Table   string `json:"table"`
		Columns []struct {
			Name string `json:"name"`
			Send int    `json:"send"`
		} `json:"columns"`
	} `json:"tables"`
}

type TaskForm struct {
	TaskId         int64                  `form:"taskid"`
	Name           string                 `form:"name" binding:"AlphaDash;MaxSize(50);Required"`
	Apiurl         string                 `form:"apiurl" binding:"Required"`
	RoutineCount   int                    `form:"routineCount" binding:"Range(1,100)"`
	ReRoutineCount int                    `form:"reRoutineCount" binding:"Range(1,100)"`
	ReSendTime     int                    `form:"reSendTime" binding:"Range(0,30000)"`
	RetryCount     int                    `form:"retryCount" binding:"Range(0,10)"`
	Timeout        int                    `form:"timeout" binding:"Range(1,30000)"`
	Desc           string                 `form:"desc" binding:"MaxSize(255)"`
	PackProtocal   model.DataPackProtocal `form:"packProtocal"`
	PhoneNunbers   string                 `form:"phoneNumbers"`
	Emails         string                 `form:"emails"`
	Alert          int                    `form:"alert"`
	Fields         []*FieldsForm          `form:"nouse"`
	AuditUser      string                 `form:"auditUser"`
}

type UserForm struct {
	Id          int64    `json:"id"`
	Username    string   `json:"username"`
	Fullname    string   `json:"fullname"`
	Permissions []string `json:"permissions"`
	Role        int      `json:"role"`
	Mail        string   `json:"mail"`
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
		Gclifetime:  3600 * 24,
		Maxlifetime: 3600 * 24,
	}))
	m.Use(gzip.Gziper())

	m.Use(func(ctx *macaron.Context, sess session.Store) {
		if ctx.Req.URL.Path == "/auth/login" {
			return
		}
		user := sess.Get("user")
		if !Conf.Debug {
			if user == nil {
				ctx.Redirect(fmt.Sprintf("%s/api/login/?camefrom=%s", Conf.WebConfig.AuthURL, Conf.WebConfig.AliasName))
			} else {
				ctx.Data["username"] = user.(*model.User).Username
				ctx.Data["user"] = user
				ctx.Data["clients"] = dispatcherManager.GetRPCClients()
			}
		} else {
			user := &model.User{
				Username: "test",
				Role:     model.USER_ADMIN,
			}
			sess.Set("user", user)
			ctx.Data["user"] = user
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
	m.Get("/task/detail/:taskid", getTaskStatistic)
	m.Get("/task/log/:taskid", loglist)
	m.Get("/help", help)

	m.Get("/task-dialog/:taskid", getEnabledTaskDialog)
	m.Get("/audit-dialog/:auditid", getAuditTaskDialog)
	m.Get("/audit", authMiddle(model.USER_AUDIT), audit)
	m.Get("/apply", authMiddle(model.USER_AUDIT), apply)

	m.Get("/user-list", authMiddle(model.USER_ADMIN), userList)
	m.Get("/user-add", authMiddle(model.USER_ADMIN), userAdd)
	m.Get("/user-edit/:id", authMiddle(model.USER_ADMIN), userEdit)

	m.Post("/audit/approve/:auditid", authMiddle(model.USER_AUDIT), auditApprove)
	m.Post("/audit/deny/:auditid", authMiddle(model.USER_AUDIT), auditDeny)
	m.Post("/audit/enable/:auditid", enableAudit)

	m.Post("/user", authMiddle(model.USER_ADMIN), binding.Bind(UserForm{}), doUserAdd)
	m.Put("/user", authMiddle(model.USER_ADMIN), binding.Bind(UserForm{}), doUserUpdate)
	m.Delete("/user", authMiddle(model.USER_ADMIN), binding.Bind(UserForm{}), doUserDelete)

	m.Post("/task", binding.Bind(TaskForm{}), doAddTask)
	m.Post("/task/changeStat/:taskid", changeTaskStat)
	m.Post("/task/copy/:taskid", copyTaskToDb)

	m.Put("/task-fields", binding.Bind(TaskForm{}), doUpdateTask)

	m.Delete("/task/:taskid", doDeleteTask)

	m.Run(Conf.WebConfig.Host, Conf.WebConfig.Port)
}

func authMiddle(flag int) func(ctx *macaron.Context, sess session.Store) {
	return func(ctx *macaron.Context, sess session.Store) {
		user := sess.Get("user").(*model.User)
		resp := &httpJsonResponse{}
		if user.Role < flag {
			resp.Error = true
			resp.Message = "权限不够"
			ctx.JSON(403, resp)
			return
		}
	}
}

//判断任务是否属于该用户
func checkTaskUser(t *model.Task, sess session.Store) bool {
	if Conf.Debug {
		return true
	}
	user := sess.Get("user").(*model.User)
	if user.Role >= model.USER_ADMIN {
		return true
	}
	return user.Username == t.CreateUser
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

func newHttpClient(isSecurity bool) *http.Client {
	var client *http.Client
	if isSecurity {
		client = http.DefaultClient
	} else {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		client = &http.Client{Transport: tr}
	}
	return client
}

func login(ctx *macaron.Context, sess session.Store) string {
	token := ctx.Query("token")
	username := ctx.Query("username")
	t := sha1.New()
	io.WriteString(t, fmt.Sprintf("%s%s%s", token, Conf.WebConfig.AppKey, username))
	sessionId := fmt.Sprintf("%x", t.Sum(nil))
	client := newHttpClient(false)
	resp, err := client.Get(fmt.Sprintf("%s/api/info/?session_id=%s", Conf.WebConfig.AuthURL, sessionId))
	if err != nil {
		return "api请求错误.#1: " + err.Error()
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "api请求错误.#2: " + err.Error()
	}
	var user UserInfo
	err = json.Unmarshal(body, &user)
	if err != nil {
		return "解析用户信息错误: " + err.Error()
	}

	apiUrl := fmt.Sprintf("%s/api/grouprole/?uid=%s&app_key=%s&app_name=%s", Conf.WebConfig.AuthURL, username, Conf.WebConfig.AppKey, Conf.WebConfig.AppName)

	resp, err = client.Get(apiUrl)
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return "api请求错误.#4: " + err.Error()
	}
	defer resp.Body.Close()
	var groups UserGroup
	err = json.Unmarshal(body, &groups)
	if err != nil {
		return "解析用户组信息错误: " + err.Error()
	}

	if len(groups.Groups) == 0 {
		return "你没有旁路平台权限."
	}
	u := &model.User{
		Username: user.UserName,
		Fullname: user.FullName,
		Mail:     user.Mail,
	}
	err = u.GetOrAdd()
	if err != nil {
		return "获取用户信息错误: " + err.Error()
	}
	sess.Set("user", u)
	ctx.Redirect("/")
	log.Infof("user login: %+v", u)
	return "登陆成功."
}

func logout(ctx *macaron.Context, sess session.Store) {
	sess.Destory(ctx)
	ctx.Redirect(fmt.Sprintf("%s/api/login/?camefrom=%s", Conf.WebConfig.AppName, Conf.WebConfig.AliasName))
}

func index(ctx *macaron.Context, sess session.Store) {
	ctx.HTML(200, "index")
}

func status(ctx *macaron.Context, sess session.Store) {
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
	client := ctx.GetCookie("client")
	colslist, _ := dispatcherManager.GetColumns(client)
	auditUsers, err := model.GetUsersByRole(model.USER_AUDIT)
	if err != nil {
		ctx.Data["error"] = err
	}
	ctx.Data["colslist"] = colslist
	ctx.Data["auditUsers"] = auditUsers
	ctx.HTML(200, "addtask")
}

func modifytask(ctx *macaron.Context, sess session.Store) {
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
	err := task.GetWithFieldsState(model.AUDIT_STATE_ENABLED)
	if err != nil {
		ctx.Data["error"] = err
	}
	auditUsers, err := model.GetUsersByRole(model.USER_AUDIT)
	if err != nil {
		ctx.Data["error"] = err
	}
	ctx.Data["auditUsers"] = auditUsers
	ctx.Data["task"] = task
	ctx.Data["taskColumnsMap"] = task.GetTaskColumnsMap()
	client := ctx.GetCookie("client")
	colslist, _ := dispatcherManager.GetColumns(client)
	ctx.Data["colslist"] = colslist

	ctx.HTML(200, "modifytask")
}

func parseFields2(fields []*FieldsForm) model.NotifyFields {
	notifyFields := make([]*model.NotifyField, 0, 10)
	for _, field := range fields {
		schemaName := field.Schema
		for _, table := range field.Tables {
			tableName := table.Table
			for _, column := range table.Columns {
				f := new(model.NotifyField)
				f.Send = column.Send
				f.Schema = schemaName
				f.Table = tableName
				f.Column = column.Name
				notifyFields = append(notifyFields, f)
			}
		}
	}
	return notifyFields
}

func doAddTask(t TaskForm, ctx *macaron.Context, sess session.Store) {
	loginUser := sess.Get("user").(*model.User)
	fields := ctx.QueryStrings("fields")
	resp := &httpJsonResponse{}
	resp.Error = false
	if len(fields) == 0 {
		resp.Error = true
		resp.Message = "请添加字段信息"
		ctx.JSON(200, resp)
		return
	}

	schema := ctx.GetCookie("client")
	rpcclient, ok := dispatcherManager.GetRPCClient(schema)
	if !ok {
		resp.Error = true
		resp.Message = "没有相应的数据库实例"
		ctx.JSON(200, resp)
		return
	}

	task := &model.Task{
		Stat:           "停止",
		CreateUser:     loginUser.Username,
		AuditState:     model.AUDIT_STATE_UNHANDLE,
		DBInstanceName: rpcclient.Desc,
	}
	copyTask(&t, task)
	if ex, _ := task.NameExists(); ex {
		resp.Error = true
		resp.Message = "任务名已经存在!"
		ctx.JSON(200, resp)
		return
	}

	task.Fields = []*model.NotifyField{}
	// 解析订阅字段信息
	for _, c := range fields {
		send := ctx.QueryInt(c)
		f := &model.NotifyField{
			Send:       send,
			AuditState: model.AUDIT_STATE_UNHANDLE,
		}
		nfs := strings.Split(c, "@@")
		if len(nfs) < 3 {
			resp.Error = true
			resp.Message = "参数错误"
			ctx.JSON(200, resp)
			return
		}
		f.Schema = nfs[0]
		f.Table = nfs[1]
		f.Column = nfs[2]
		if !FieldExists(task.Fields, f) {
			task.Fields = append(task.Fields, f)
		}
	}

	// 生成审核信息
	audit := &model.Audit{
		ApplyUser: loginUser.Username,
		ApplyType: model.AUDIT_TYPE_CREATE,
		AuditUser: t.AuditUser,
		State:     model.AUDIT_STATE_UNHANDLE,
	}

	err := model.AddTaskWithAudit(task, audit)
	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
		ctx.JSON(200, resp)
		return
	}
	log.Printf("%s: add task %+v", loginUser.Username, task)
	ctx.JSON(200, resp)
}

func doDeleteTask(ctx *macaron.Context, sess session.Store) string {
	loginUser := sess.Get("user").(*model.User)
	id := ctx.ParamsInt64("taskid")
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
		log.Error("delete task: ", err.Error())
	} else {
		resp.Message = "删除成功"
	}
	dispatcherManager.DeleteTask(task)
	pusherManager.DeleteTask(task)
	body, _ := json.Marshal(resp)
	ctx.Resp.WriteHeader(204)
	log.Printf("%s: delete task %v", loginUser.Username, task.Name)
	return string(body)
}

func doUpdateTask(t TaskForm, ctx *macaron.Context, sess session.Store) {
	loginUser := sess.Get("user").(*model.User)
	taskid := ctx.QueryInt64("taskid")
	resp := &httpJsonResponse{}
	task := &model.Task{
		ID: taskid,
	}
	if !checkTaskUser(task, sess) {
		resp.Error = true
		resp.Message = "你无权操作"
		ctx.JSON(200, resp)
		return
	}

	fields := ctx.QueryStrings("fields")
	if len(fields) == 0 {
		resp.Error = true
		resp.Message = "请添加字段信息"
		ctx.JSON(200, resp)
		return
	}
	err := task.GetById()
	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
		ctx.JSON(200, resp)
		return
	}
	if task.Name != t.Name {
		resp.Error = true
		resp.Message = "名字不能修改"
		ctx.JSON(200, resp)
		return
	}
	copyTask(&t, task)
	isUpdateFields := ctx.QueryInt("isUpdateFields")
	if isUpdateFields == 1 {
		task.Fields = []*model.NotifyField{}
		for _, c := range fields {
			send := ctx.QueryInt(c)
			f := new(model.NotifyField)
			f.Send = send
			f.AuditState = model.AUDIT_STATE_UNHANDLE
			nfs := strings.Split(c, "@@")
			if len(nfs) < 3 {
				resp.Error = true
				resp.Message = "参数错误"
				ctx.JSON(200, resp)
				return
			}
			f.Schema = nfs[0]
			f.Table = nfs[1]
			f.Column = nfs[2]
			if !FieldExists(task.Fields, f) {
				task.Fields = append(task.Fields, f)
			}
		}

		audit := &model.Audit{
			ApplyUser: loginUser.Username,
			ApplyType: model.AUDIT_TYPE_UPDATE,
			AuditUser: t.AuditUser,
			State:     model.AUDIT_STATE_UNHANDLE,
		}
		err := model.UpdateTaskFieldsWithAudit(task, audit)
		if err != nil {
			resp.Error = true
			resp.Message = err.Error()
			ctx.JSON(200, resp)
			return
		}

		resp.Message = "提交审核成功!"
		ctx.JSON(200, resp)

	} else {
		err := task.Update()
		if err != nil {
			resp.Error = true
			resp.Message = err.Error()
			log.Error("do update task: ", err.Error())
		} else {
			resp.Message = "更新成功!"
		}
		ctx.JSON(200, resp)
	}
	log.Printf("%s: update task %+v", loginUser.Username, task)

}

func tasklist(ctx *macaron.Context, sess session.Store) {
	var sortTasks []*model.Task
	schema := ctx.GetCookie("client")
	client, ok := dispatcherManager.GetRPCClient(schema)
	if !ok {
		ctx.HTML(200, "tasklist")
		return
	}
	var err error
	sortTasks, err = model.GetEnabledTasksByInstance(client.Desc)
	if err != nil {
		log.Error("get task by instance name: ", err.Error())
	}
	sort.Sort(TaskSlice(sortTasks))
	ctx.Data["tasks"] = sortTasks
	ctx.HTML(200, "tasklist")
}

func changeTaskStat(ctx *macaron.Context, sess session.Store) {
	loginUser := sess.Get("user").(*model.User)
	taskid := ctx.ParamsInt64("taskid")
	resp := new(httpJsonResponse)
	resp.Error = false
	task := &model.Task{
		ID: taskid,
	}
	err := task.GetById()
	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
		ctx.JSON(200, err.Error())
		return
	}
	if !checkTaskUser(task, sess) {
		resp.Error = true
		resp.Message = "权限不够"
		ctx.JSON(403, resp)
		return
	}
	if task.AuditState != model.AUDIT_STATE_ENABLED {
		resp.Error = true
		resp.Message = "该任务未执行！"
		ctx.JSON(200, resp)
		return
	}
	stat := ctx.Query("stat")
	task.Stat = stat
	err = task.UpdateStat()
	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
		log.Error("change task stat: ", err.Error())
	} else {
		resp.Error = false
		resp.Message = "操作成功"
	}
	if stat == model.TASK_STATE_START {
		nsqManager.UnPauseTopic(task.Name)
		dispatcherManager.StartTask(task)
		pusherManager.StartTask(task)
	} else if stat == model.TASK_STATE_STOP {
		nsqManager.PauseTopic(task.Name)
		dispatcherManager.StopTask(task)
		pusherManager.StopTask(task)
	}

	log.Printf("%s: change task %s state to %s ", loginUser.Username, task.Name, task.Stat)
	ctx.JSON(200, resp)
}

func loglist(ctx *macaron.Context, sess session.Store) {
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
	tls, err := model.GetTaskLogByTaskId(taskid, 0, 20)
	if err != nil {
		log.Error("get task log by task id: ", err.Error())
	}
	for _, tl := range tls {
		tl.CreateTime = tl.CreateTime.Local()
	}
	ctx.Data["task"] = task
	ctx.Data["logs"] = tls
	ctx.HTML(200, "loglist")
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
	dst.PackProtocal = src.PackProtocal
	dst.PhoneNumbers = src.PhoneNunbers
	dst.Emails = src.Emails
	dst.Alert = src.Alert
}

func FieldExists(fields []*model.NotifyField, field *model.NotifyField) bool {
	for _, f := range fields {
		if f.Schema == field.Schema && f.Table == field.Table && f.Column == field.Column {
			return true
		}
	}
	return false
}

func getTaskStatistic(ctx *macaron.Context, sess session.Store) {
	taskid := ctx.ParamsInt64("taskid")
	task := &model.Task{
		ID: taskid,
	}
	if ext, _ := task.Exists(); !ext {
		ctx.HTML(404, "404")
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

func help(ctx *macaron.Context, sess session.Store) {
	ctx.HTML(200, "help")
}

func copyTaskToDb(ctx *macaron.Context, sess session.Store) string {
	loginUser := sess.Get("user").(*model.User)
	taskid := ctx.ParamsInt64("taskid")
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
	db := ctx.Query("dbInstanceName")
	name := ctx.Query("taskName")
	stat := ctx.Query("stat")
	rpcclient, ok := dispatcherManager.GetRPCClient(db)
	if !ok {
		resp.Error = true
		resp.Message = "没有相应的数据库实例"
		body, _ := json.Marshal(resp)
		return string(body)
	}
	task.DBInstanceName = rpcclient.Desc
	task.Name = name
	task.Stat = stat
	task.CreateUser = sess.Get("user").(string)
	if ex, _ := task.NameExists(); ex {
		resp.Error = true
		resp.Message = "任务名已经存在!"
		body, _ := json.Marshal(resp)
		return string(body)
	}
	err := task.Add()
	if err != nil {
		resp.Error = true
		resp.Message = "复制失败!"
		log.Errorf("add task: %s", err.Error())
	} else {
		resp.Message = "复制成功!"
	}
	dispatcherManager.AddTask(task)
	pusherManager.AddTask(task)
	body, _ := json.Marshal(resp)
	ctx.Resp.WriteHeader(201)
	log.Printf("%s: copy task %s", loginUser.Username, task.Name)
	return string(body)
}

func audit(ctx *macaron.Context, sess session.Store) {
	user := sess.Get("user").(*model.User)
	audit, err := model.GetAuditByAuditUser(user.Username)
	ctx.Data["error"] = err
	ctx.Data["audits"] = audit
	ctx.HTML(200, "audit")
}

func apply(ctx *macaron.Context, sess session.Store) {
	user := sess.Get("user").(*model.User)
	audit, err := model.GetAuditByApplyUser(user.Username)
	ctx.Data["error"] = err
	ctx.Data["audits"] = audit
	ctx.HTML(200, "apply")
}

func userList(ctx *macaron.Context, sess session.Store) {
	users, err := model.GetAllUsers()
	ctx.Data["users"] = users
	ctx.Data["error"] = err
	ctx.HTML(200, "user_list")
}

func userAdd(ctx *macaron.Context, sess session.Store) {
	ctx.HTML(200, "user_add")
}

func userEdit(ctx *macaron.Context, sess session.Store) {
	id := ctx.ParamsInt64("id")
	user := &model.User{
		Id: id,
	}
	err := user.GetById()
	ctx.Data["updateUser"] = user
	ctx.Data["error"] = err
	ctx.HTML(200, "user_edit")
}

func doUserAdd(u UserForm, ctx *macaron.Context, sess session.Store) {
	loginUser := sess.Get("user").(*model.User)
	resp := &httpJsonResponse{}
	user := &model.User{
		Username:    u.Username,
		Fullname:    u.Fullname,
		Permissions: strings.Join(u.Permissions, ","),
		Role:        u.Role,
		Mail:        u.Mail,
	}
	exists, err := user.NameExists()
	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
		ctx.JSON(200, resp)
		return
	}
	if exists {
		resp.Error = true
		resp.Message = "用户名已存在"
		ctx.JSON(200, resp)
		return
	}
	err = user.Add()

	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
	} else {
		resp.Error = false
		resp.Message = "添加成功!"
	}
	log.Infof("%s add user %+v", loginUser.Username, user)
	ctx.JSON(200, resp)
}

func doUserUpdate(u UserForm, ctx *macaron.Context, sess session.Store) {
	loginUser := sess.Get("user").(*model.User)
	user := &model.User{
		Id:   u.Id,
		Role: u.Role,
	}
	_, err := user.UpdateRole()
	resp := &httpJsonResponse{}
	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
	} else {
		resp.Error = false
		resp.Message = "更新成功!"
	}
	log.Infof("%s update user: %+v", loginUser.Username, user)
	ctx.JSON(200, resp)
}

func doUserDelete(u UserForm, ctx *macaron.Context, sess session.Store) {
	loginUser := sess.Get("user").(*model.User)
	user := &model.User{
		Id: u.Id,
	}
	_, err := user.Delete()
	resp := &httpJsonResponse{}

	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
	} else {
		resp.Error = false
		resp.Message = "删除成功!"
	}
	log.Infof("%s delete user: %+v", loginUser.Username, user)
	ctx.JSON(200, resp)
}

func getEnabledTaskDialog(ctx *macaron.Context, sess session.Store) {
	taskid := ctx.ParamsInt64("taskid")
	task := &model.Task{
		ID: taskid,
	}

	err := task.GetWithFieldsState(model.AUDIT_STATE_ENABLED)
	if err != nil {
		log.Errorf("get audit task: %s", err.Error())
	}
	ctx.Data["task"] = task
	ctx.Data["Id"] = taskid
	ctx.HTML(200, "taskDialog")
}

func getAuditTaskDialog(ctx *macaron.Context, sess session.Store) {
	auditid := ctx.ParamsInt64("auditid")
	audit := &model.Audit{
		Id: auditid,
	}
	err := audit.GetById()
	if err != nil {
		ctx.HTML(200, "taskDialog")
		return
	}
	task, err := model.GetTaskFieldsByAudit(audit)
	if err != nil {
		log.Errorf("get audit task: %s", err.Error())
	}
	ctx.Data["task"] = task
	ctx.Data["Id"] = auditid
	ctx.HTML(200, "taskDialog")
}

func auditApprove(ctx *macaron.Context, sess session.Store) {
	loginUser := sess.Get("user").(*model.User)
	resp := &httpJsonResponse{}

	id := ctx.ParamsInt64("auditid")
	audit := &model.Audit{
		Id: id,
	}
	err := audit.GetById()
	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
		ctx.JSON(200, resp)
		return
	}
	if audit.AuditUser != loginUser.Username {
		resp.Error = true
		resp.Message = "你无权操作"
		ctx.JSON(200, resp)
		return
	}
	audit.State = model.AUDIT_STATE_APPROVED
	err = model.UpdateAuditState(audit)
	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
		ctx.JSON(200, resp)
		return
	}

	resp.Message = "操作成功!"
	log.Infof("%s audit approve %+v", loginUser.Username, audit)
	ctx.JSON(200, resp)
}

func auditDeny(ctx *macaron.Context, sess session.Store) {
	loginUser := sess.Get("user").(*model.User)
	resp := &httpJsonResponse{}

	id := ctx.ParamsInt64("auditid")
	audit := &model.Audit{
		Id:    id,
		State: model.AUDIT_STATE_DENYED,
	}

	err := audit.GetById()
	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
		ctx.JSON(200, resp)
		return
	}
	if audit.AuditUser != loginUser.Username {
		resp.Error = true
		resp.Message = "你无权操作"
		ctx.JSON(200, resp)
		return
	}
	err = model.UpdateAuditState(audit)
	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
		ctx.JSON(200, resp)
		return
	}

	resp.Message = "操作成功!"
	log.Infof("%s audit deny %+v", loginUser.Username, audit)
	ctx.JSON(200, resp)
}

func enableAudit(ctx *macaron.Context, sess session.Store) {
	loginUser := sess.Get("user").(*model.User)
	auditid := ctx.ParamsInt64("auditid")
	resp := &httpJsonResponse{}
	audit := &model.Audit{
		Id: auditid,
	}
	err := audit.GetById()
	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
		return
	}
	audit.State = model.AUDIT_STATE_ENABLED
	err = model.EnableAudit(audit)
	if err != nil {
		resp.Error = true
		resp.Message = err.Error()
		ctx.JSON(200, resp)
		return
	}
	resp.Message = "操作成功!"
	// TODO: 发送到dispatcher和pusher
	log.Infof("%s enabled audit %+v", loginUser.Username, audit)
	ctx.JSON(200, resp)
}
