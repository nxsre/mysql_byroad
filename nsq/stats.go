package nsqm

// stats with envelope.
type stats struct {
	Data *Stats
}

// nsqd 的stats接口统计信息
type Stats struct {
	Version string   `json:"version"`
	Health  string   `json:"health"`
	Topics  []*Topic `json:"topics"`
}

//
type NodeStats struct {
	Producer *Producer
	Stats    *Stats
}

// nsqlookupd的lookup接口返回信息
type Node struct {
	Producer
	Tombstones []bool   `json:"tombstones"`
	Topics     []string `json:"topics"`
}

// nsqlookupd的topics接口返回信息
type Producer struct {
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

// topic在一个nsqd上的统计信息
type TopicStats struct {
	Producer *Producer
	Topic    *Topic
}

// Topic stats.
type Topic struct {
	Name          string     `json:"topic_name"`
	InFlightCount int64      `json:"in_flight_count"`
	DeferredCount int64      `json:"deferred_count"`
	MessageCount  int64      `json:"message_count"`
	RequeueCount  int64      `json:"requeue_count"`
	TimeoutCount  int64      `json:"timeout_count"`
	BackendDepth  int64      `json:"backend_depth"`
	Depth         int64      `json:"depth"`
	Paused        bool       `json:"paused"`
	Channels      []*Channel `json:"channels"`
}

// Channel stats.
type Channel struct {
	Name          string `json:"channel_name"`
	InFlightCount int64  `json:"in_flight_count"`
	DeferredCount int64  `json:"deferred_count"`
	MessageCount  int64  `json:"message_count"`
	RequeueCount  int64  `json:"requeue_count"`
	TimeoutCount  int64  `json:"timeout_count"`
	BackendDepth  int64  `json:"backend_depth"`
	Depth         int64  `json:"depth"`
	Paused        bool   `json:"paused"`
}
