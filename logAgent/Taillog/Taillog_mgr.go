package Taillog

import (
	"fmt"
	"studyGo/logAgent/Etcd"
	"time"
)

//TailTask的管理者
type taillogMgr struct {
	LogEntry    []*Etcd.LoggerConf
	tskMap      map[string]*TailTask
	newConfChan chan []*Etcd.LoggerConf //存放最新配置信息的通道

}

var tskMgr *taillogMgr

func Init(loggerConf []*Etcd.LoggerConf) {

	tskMgr = &taillogMgr{
		LogEntry:    loggerConf,
		tskMap:      make(map[string]*TailTask, 24),
		newConfChan: make(chan []*Etcd.LoggerConf), //无缓冲区的通道

	}
	//4. 根据配置信息打开对应日志文件的路径以及Kafka内对应的topic
	for _, value := range loggerConf {
		tailObj := NewTailTask(value.Path, value.Topic)
		mk := fmt.Sprintf("%s_%s", value.Path, value.Topic)
		//在程序第一次运行时获取初始配置并保存，在之后用来比较实现热更新
		tskMgr.tskMap[mk] = tailObj
	}
	go tskMgr.run()
}

//监听newChanConf，有了新的配置就处理
func (t *taillogMgr) run() {
	for {
		select {
		case newConf := <-t.newConfChan:
			//当添加一个配置项时
			for _, conf := range newConf {
				mk := fmt.Sprintf("%s_%s", conf.Path, conf.Topic)
				_, ok := t.tskMap[mk]
				if !ok {
					fmt.Println("新的配置信息来了", newConf)
					//没有在t.tskMap内找到相应的配置说明是新增的则需要创建一个新的tailObj，并保存
					tailObj := NewTailTask(conf.Path, conf.Topic)
					t.tskMap[mk] = tailObj
				}
			}
			//当删除一个配置项时
			for _, c1 := range t.LogEntry {
				isDelete := true
				for _, c2 := range newConf {
					if c1.Path == c2.Path && c1.Topic == c2.Topic {
						isDelete = false
						continue
					}
					if isDelete {
						mk := fmt.Sprintf("%s_%s", c1.Path, c1.Topic)
						tskMgr.tskMap[mk].cancelFunc()
					}
				}
			}
		default:
			time.Sleep(time.Second)
		}
	}
}

//一个函数，向外暴露tskMgr的newChanConf字段
func NewConfChan() chan<- []*Etcd.LoggerConf {
	return tskMgr.newConfChan
}
