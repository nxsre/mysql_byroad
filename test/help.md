## 消息中对mysql字段类型为text和blob的处理

mysql对text类型的字段在binlog传输时，是当做二进制数据的，因此，在接收到binlog时， 
不能对text类型的数据当做字符串处理。在消息中，**对数据进行json处理时，会进行base64编码**，在接收到消息时，请自行解码还原。

## 任务添加时使用正则表达式

在对任务订阅的字段进行添加时，可以对数据库名和表名使用正则表达式。例如`jumei_product`库的`tuanmei_deals`表有很多sharding，
如`jumei_product_1.tuammei_deals_1`这样的形式。在订阅时，可以对数据库名和字段名进行编辑，
如`jumei_product_\d+.tuanmei_deals_\d+`，这将会匹配所有的sharding。

## 推送的消息格式
```golang
type NotifyEvent struct {
	Event        string         `json:"event"`      //消息事件类型：Insert, Delete, Update
	Schema       string         `json:"schema"`     //数据库名
	Table        string         `json:"table"`      //表名
	Fields       []*ColumnValue `json:"fields"`     // 变化的字段信息
	Keys         []string       `json:"keys"`       //不推送值的字段名称
	RetryCount   int            `json:"retryCount"` // 重推次数
	LastSendTime time.Time      `json:"lastSendTime"` //推送的时间
	TaskID       int64          `json:"taskID"`     //任务ID
}
type ColumnValue struct {
	ColunmName string      `json:"columnName"`
	Value      interface{} `json:"value"`
	OldValue   interface{} `json:"oldValue"`
}
```  
消息示例：
```json
{
    "event": "Update",
    "schema": "test",
    "table": "user",
    "fields": [
        {
            "columnName": "id",
            "value": "10",
            "oldValue": "10"
        },
        {
            "columnName": "password",
            "value": "123456",
            "oldValue": "admin"
        }
    ],
    "keys": [
        "username",
        "address"
    ],
    "retryCount": 10,
    "lastSendTime": "2016-08-09T10:16:16.516767809+08:00",
    "taskID": 41
}
```

## 更新日志

### 2016.10.21更新
1. 支持在网络出现异常时，重连mysql。由于网路出现异常，例如网络断开等情况时，replication client端不会收到任何消息，使得程序一直阻塞在读取网络数据上，
网络恢复后，服务端的连接已经断开，client将收不到任何数据，一直堵塞。通过设置超时的重连机制，在client长时间收不到消息的时候，会认为网络出现了问题，将会进行重连操作
2. 支持正则表达式，在数据库名和表名的设置中，可以使用正则表达式，在进行匹配时，将会在表达式前后自动添加`^`和`$`，因此之前未使用`*`作为通配的将不会有影响，使用了`*`
号的需要将`*`号改为`\w+`或者`\d+`之类的正则表达式
3. 对`enum`类型的字段支持，由于`enum`和`set`类型的字段在binlog使用的是序号，而不是`enum`的值，因此需要读取`schema`来获取`enum`序号对应的值