package model

import "time"

type TaskLog struct {
	Id         int64     `db:"id"`
	TaskId     int64     `db:"task_id"`
	Message    string    `db:"message"`
	Reason     string    `db:"reason"`
	CreateTime time.Time `db:"create_time"`
}

func createTaskLogTable() {
	sql := `CREATE TABLE IF NOT EXISTS tasklog(
		id INTEGER PRIMARY KEY AUTO_INCREMENT,
		task_id INTEGER NOT NULL,
		message VARCHAR(1000),
		reason VARCHAR(10000),
		create_time DATETIME
	)`
	confdb.MustExec(sql)
}

func (tl *TaskLog) Insert() (int64, error) {
	sql := `INSERT INTO tasklog (task_id, message, reason, create_time) VALUES (?,?,?,?)`
	ret, err := confdb.Exec(sql, tl.TaskId, tl.Message, tl.Reason, tl.CreateTime)
	if err != nil {
		return 0, err
	}
	return ret.LastInsertId()
}

func GetTaskLogByTaskId(id, start, offset int64) ([]*TaskLog, error) {
	tls := []*TaskLog{}
	err := confdb.Select(&tls, "SELECT * FROM tasklog WHERE task_id=? ORDER BY create_time desc LIMIT ?,?", id, start, offset)
	return tls, err
}
