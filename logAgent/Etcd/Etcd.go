package Etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	cli *clientv3.Client
)

type LoggerConf struct {
	Path  string `json:"Path"`
	Topic string `json:"Topic"`
}

//初始化etcd连接
func Init(addr string, timeOut time.Duration) (err error) {
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: timeOut,
	})
	if err != nil {
		fmt.Println("connect to etcd failed,err:", err)
		return
	}
	return
}

//根据Key在etcd内找到相应的配置文件
func EtcdConfig(key string) (loggerConf []*LoggerConf, err error) {
	ctx, cancle := context.WithTimeout(context.Background(), time.Second)
	GetResp, err := cli.Get(ctx, key)
	cancle()
	if err != nil {
		fmt.Println("Get key dailed,err:", err)
		return
	}
	for _, ev := range GetResp.Kvs {
		err = json.Unmarshal(ev.Value, &loggerConf)
		fmt.Printf("%s:%s\n", ev.Key, ev.Value)
		if err != nil {
			fmt.Println("Unmarshal etcd values failed,err:", err)
			return
		}
	}
	return
}

//监听配置信息
func WatchConf(key string, newConfChan chan<- []*LoggerConf) {
	listener := cli.Watch(context.Background(), key)
	for ch := range listener {
		for _, evt := range ch.Events {
			fmt.Printf("Type:%v Key:%v Value:%v\n", evt.Type, string(evt.Kv.Key), string(evt.Kv.Value))
			var newConf []*LoggerConf
			if evt.Type != clientv3.EventTypeDelete {
				err := json.Unmarshal(evt.Kv.Value, &newConf)
				if err != nil {
					fmt.Println("unmarshal failed.err:", err)
					continue
				}
			}
			fmt.Println("get new config", newConf)
			newConfChan <- newConf
		}
	}
}
