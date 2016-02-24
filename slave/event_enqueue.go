package slave

import (
	"mysql-slave/common"
	"sync"
)

type EventEnqueuer struct {
	sync.WaitGroup
}

func NewEventEnqueue() *EventEnqueuer {
	return &EventEnqueuer{}
}

/*
根据任务数量并发的将消息写入redis中
*/
func (this *EventEnqueuer) Enqueue(schema, table, event string, taskFieldMap map[int64][]*common.ColumnValue) {
	for taskid, fields := range taskFieldMap {
		this.Add(1)
		go this.enqueue(schema, table, event, taskid, fields)
	}
	this.Wait()
}

func (this *EventEnqueuer) enqueue(schema, table, event string, taskid int64, fields []*common.ColumnValue) {
	ntyevt := new(common.NotifyEvent)
	ntyevt.Keys = make([]string, 0)
	ntyevt.Fields = make([]*common.ColumnValue, 0)
	task := GetTask(taskid)
	if task == nil {
		this.Done()
		return
	}
	updateChanged := false
	for _, f := range fields {
		tf := task.GetField(schema, table, f.ColunmName)
		if tf == nil {
			continue
		}
		if event != common.UPDATE_EVENT {
			if tf.Send == 1 {
				ntyevt.Fields = append(ntyevt.Fields, f)
			} else {
				ntyevt.Keys = append(ntyevt.Keys, f.ColunmName)
			}
		} else {
			if tf.Send == 1 {
				ntyevt.Fields = append(ntyevt.Fields, f)
				if !isEqual(f.Value, f.OldValue) {
					updateChanged = true
				}
			} else if !isEqual(f.Value, f.OldValue) {
				ntyevt.Keys = append(ntyevt.Keys, f.ColunmName)
				updateChanged = true
			}
		}
	}
	if len(ntyevt.Fields) == 0 && len(ntyevt.Keys) == 0 {
		this.Done()
		return
	} else if event == common.UPDATE_EVENT && !updateChanged {
		this.Done()
		return
	}
	ntyevt.Schema = schema
	ntyevt.Table = table
	ntyevt.Event = event
	ntyevt.TaskID = task.ID
	name := genTaskQueueName(task)
	queueManager.Enqueue(name, ntyevt)
	this.Done()
}
