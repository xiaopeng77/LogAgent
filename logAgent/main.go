package main

import (
	"fmt"
	"studyGo/logAgent/Config"
	"studyGo/logAgent/Etcd"
	"studyGo/logAgent/Taillog"
	"studyGo/logAgent/kafka"
	"studyGo/logAgent/utils"
	"sync"
	"time"

	"gopkg.in/ini.v1"
)

var (
	config = new(Config.ConfigAdd)
)

func main() {
	//加载配置文件
	err := ini.MapTo(config, "./config.ini")
	if err != nil {
		fmt.Println("ini configfile failed,err:", err)
		return
	}
	//1. 初始化Kafka连接
	err = kafka.Init([]string{config.KafkaConf.Address}, config.KafkaConf.ChanMaxSize)
	if err != nil {
		fmt.Println("init Kagka failed,err:", err)
		return
	}
	fmt.Println("init Kafka success!")

	//2.1 初始化etcd连接
	err = Etcd.Init(config.EtcdConf.Address, time.Duration(config.EtcdConf.Timeout)*time.Second)
	if err != nil {
		fmt.Println("init Etcd failed,err:", err)
		return
	}
	fmt.Println("init Etcd success!")
	//2.2 获取本机对外的IP地址
	IP, err := utils.GetOutboundIP()
	if err != nil {
		panic(err)
	}
	strIP := fmt.Sprintf(config.EtcdConf.Address, IP)
	//3. 从etcd中获取日志收集配置信息
	loggerConf, err := Etcd.EtcdConfig(strIP)
	if err != nil {
		fmt.Println("Etcd.EtcdConfig failed,err:", err)
		return
	}
	fmt.Println(loggerConf)
	fmt.Println("get conf from etcd success!")

	//4. 收集日志信息发往Kafka
	Taillog.Init(loggerConf)
	ConfChan := Taillog.NewConfChan() //从taillog包中获取对外暴露的通道
	var wg sync.WaitGroup
	wg.Add(1)
	//5. 起一个后台goroutine去监听配置信息是否改变
	go Etcd.WatchConf(strIP, ConfChan) //哨兵发现最新的配置信息会通知上面的那个通道
	wg.Wait()

}
