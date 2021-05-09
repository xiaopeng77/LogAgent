package kafka

import (
	"fmt"
	"studyGo/log_transfer/es"

	"github.com/Shopify/sarama"
)

//初始化一个消费实例连接
//初始化一个kafka对象,并发往ES
func Init(addrs []string, topic string) error {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          //发送完数据需要leader和follower都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner //新选出一个partition
	config.Producer.Return.Successes = true                   //成功交付的消息将在success channel返回
	//连接kafka
	consumer, err := sarama.NewConsumer(addrs, nil)
	if err != nil {
		fmt.Println("start consumer failed", err)
		return err
	}
	//创建一个消费者实例
	partitionList, err := consumer.Partitions(topic) //根据topic取到所有的分区
	if err != nil {
		fmt.Println("get partition failed", err)
		return err
	}
	fmt.Println(partitionList)
	for partition := range partitionList { //遍历所有的分区
		//针对每一个分区创建一个对应的消费实例
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d,err:%s", partition, err)
			return err
		}
		defer pc.AsyncClose()
		//异步从每个分区消费实例
		go func(sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				fmt.Println("Partition:", msg.Partition, "Offset:", msg.Offset, "Key:", msg.Key, "Value:", msg.Value)
				//发给ES
				ld := es.LogData{
					Topic: topic,
					Data:  string(msg.Value),
				}
				es.SendToChan(&ld)
			}
		}(pc)

	}
	return nil
}
