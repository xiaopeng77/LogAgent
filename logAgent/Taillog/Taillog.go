package Taillog

import (
	"context"
	"fmt"
	"time"

	"studyGo/logAgent/kafka"

	"github.com/hpcloud/tail"
)

//获取日志信息
var (
	Tails *tail.Tail
)

type TailTask struct {
	path     string
	topic    string
	instance *tail.Tail
	//利用context来控制当配置删除时结束对应的先Kafka内写日志的函数
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewTailTask(path, topic string) (tailObj *TailTask) {
	ctx, cancel := context.WithCancel(context.Background())
	tailObj = &TailTask{
		path:       path,
		topic:      topic,
		ctx:        ctx,
		cancelFunc: cancel,
	}
	tailObj.init() //根据路径打开对应的日志文件
	return
}

//初始化tail对象
func (t *TailTask) init() {
	config := tail.Config{
		ReOpen:    true,                                 // 重新打开
		Follow:    true,                                 // 是否跟随
		MustExist: false,                                // 文件不存在报错
		Poll:      true,                                 // Poll for file changes instead of using inotify
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, //从文件的哪个地方开始读
	}
	var err error
	t.instance, err = tail.TailFile(t.path, config)
	if err != nil {
		fmt.Println("tail file failed,err:", err)
	}
	go t.run() //直接采集日志发给Kafka
}

func (t *TailTask) run() {
	for {
		select {
		case <-t.ctx.Done():
			fmt.Printf("tail task %s exit... ", t.path+t.topic)
			return
		case line := <-t.instance.Lines: //从tailObj里面一行一行的去读取日志
			kafka.SendtoChan(t.topic, line.Text) //发送数据到Kafka
		default:
			time.Sleep(time.Millisecond * 50)
		}
	}
}
