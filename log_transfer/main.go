package main

import (
	"fmt"
	"studyGo/log_transfer/config"
	"studyGo/log_transfer/es"
	"studyGo/log_transfer/kafka"

	"gopkg.in/ini.v1"
)

//日志项目的消费实例
//将日志信息从Kafka取出发往ES

var (
	cfg = new(config.LogTransfer)
)

func main() {
	//0. 加载配置文件
	err := ini.MapTo(cfg, "./config.ini")
	if err != nil {
		fmt.Println("ini configfile failed,err:", err)
		return
	}
	fmt.Printf("cfg:%v", cfg)
	//1. 初始化ES连接
	//1.1 初始化一个ES连接的client
	err = es.Init(cfg.EScfg.Address, cfg.EScfg.ChanMaxSize, cfg.EScfg.Nums)
	if err != nil {
		fmt.Println("init es failed,err:", err)
		return
	}
	fmt.Println("init es success")
	//2. 初始化Kafka消费实例
	//2.1 连接Kafka创建分区的消费者
	//2.2 每个分区的消费者取出数据，通过SendToES()发给ES
	err = kafka.Init([]string{cfg.Kafkacfg.Address}, cfg.Kafkacfg.Topic)
	if err != nil {
		fmt.Println("init kafka failed,err:", err)
		return
	}

}
