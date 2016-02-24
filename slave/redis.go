package slave

import (
	"encoding/json"
	"mysql-slave/common"
	"time"

	"github.com/garyburd/redigo/redis"
)

/*
根据任务的名字来将消息放入不同的redis队列
*/
type QueueManger struct {
	redisPool *redis.Pool
}

func NewQueueManager(config *common.RedisConfig) *QueueManger {
	redisPool := newPool(config.Host+":"+config.Port, config.Password, config.MaxIdle, config.MaxActive)
	return &QueueManger{
		redisPool: redisPool,
	}
}

func newPool(server, password string, maxIdle, maxActive int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     maxIdle,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Wait:      true,
		MaxActive: maxActive,
	}
}

func (m *QueueManger) Enqueue(name string, evt interface{}) {
	conn := m.redisPool.Get()
	defer conn.Close()
	evtStr, err := json.Marshal(evt)
	if err != nil {
		sysLogger.LogErr(err)
		return
	}
	_, err = conn.Do("LPUSH", name, evtStr)
	if err != nil {
		owl.LogThisException(err.Error())
	}
	sysLogger.LogErr(err)
}

/*
有阻塞的出队列,阻塞时间1s
*/
func (m *QueueManger) Dequeue(name string) interface{} {
	conn := m.redisPool.Get()
	defer conn.Close()
	v, err := conn.Do("BRPOP", name, 1)
	if err != nil {
		sysLogger.LogErr(err)
		owl.LogThisException(err.Error())
		return nil
	}
	if v != nil {
		switch t := v.(type) {
		case []interface{}:
			if len(t) == 2 {
				return t[1]
			}
		default:
		}
	}
	return nil
}

/*
无阻塞的出队列,如果没有值，返回nil
*/
func (m *QueueManger) NBDequeue(name string) interface{} {
	conn := m.redisPool.Get()
	defer conn.Close()
	v, err := conn.Do("RPOP", name)
	if err != nil {
		sysLogger.LogErr(err)
		return nil
	}
	return v
}

func (m *QueueManger) Empty(name string) {
	conn := m.redisPool.Get()
	defer conn.Close()
	l := m.Len(name)
	_, err := conn.Do("LTRIM", name, l+1, -1)
	sysLogger.LogErr(err)
	if err != nil {
		owl.LogThisException(err.Error())
	}
}

/*
获取队列的长度
*/
func (m *QueueManger) Len(name string) int64 {
	conn := m.redisPool.Get()
	defer conn.Close()
	v, err := conn.Do("LLEN", name)
	if err != nil {
		sysLogger.LogErr(err)
		owl.LogThisException(err.Error())
		return 0
	}
	return v.(int64)
}

/*
获取任务的推送队列和重推队列的大小
*/
func (m *QueueManger) TaskQueueLen(t *Task) []int64 {
	conn := m.redisPool.Get()
	defer conn.Close()
	name := genTaskQueueName(t)
	rename := genTaskReQueueName(t)
	conn.Send("LLEN", name)
	conn.Send("LLEN", rename)
	res := make([]int64, 2)
	v1, err := conn.Receive()
	sysLogger.LogErr(err)
	if err != nil {
		owl.LogThisException(err.Error())
		return res
	}
	v2, err := conn.Receive()
	sysLogger.LogErr(err)
	if err != nil {
		owl.LogThisException(err.Error())
		return res
	}
	res[0] = v1.(int64)
	res[1] = v2.(int64)
	return res
}

/*
获取所有任务的推送队列和重推队列的大小
*/
func (m *QueueManger) TasksQueueLen(ts []*Task) [][]int64 {
	conn := m.redisPool.Get()
	defer conn.Close()
	res := make([][]int64, 0, 100)
	conn.Send("MULTI")
	for _, t := range ts {
		name := genTaskQueueName(t)
		rename := genTaskReQueueName(t)
		conn.Send("LLEN", name)
		conn.Send("LLEN", rename)
	}
	rets, err := conn.Do("EXEC")
	sysLogger.LogErr(err)
	if err != nil {
		owl.LogThisException(err.Error())
		return res
	}
	returns := rets.([]interface{})
	l := len(returns)
	for i := 0; i < l; i += 2 {
		t := ts[i/2]
		r := make([]int64, 2)
		v := returns[i]
		r[0] = v.(int64)
		t.QueueLength = v.(int64)
		v = returns[i+1]
		r[1] = v.(int64)
		t.ReQueueLength = v.(int64)
		res = append(res, r)
	}
	return res
}

func (m *QueueManger) Clean() error {
	return m.redisPool.Close()
}
