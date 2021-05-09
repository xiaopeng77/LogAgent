package kafka

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

type logDate struct {
	topic string
	data  string
}

//往kafka内写日志
var (
	client      sarama.SyncProducer //声明一个全局的连接Kafka的生产者client
	logDateChan chan *logDate       //存放topic以及日志数据的一个通道
)

//初始化一个kafka对象
func Init(addrs []string, MaxSize int) (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          //发送完数据需要leader和follower都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner //新选出一个partition
	config.Producer.Return.Successes = true                   //成功交付的消息将在success channel返回
	//连接kafka
	client, err = sarama.NewSyncProducer(addrs, config)
	if err != nil {
		fmt.Println("producer closed,err:", err)
		return
	}
	logDateChan = make(chan *logDate, MaxSize) //为logDateChan分配内存
	go sendtoKafka()                           //开启后台的goroutine从通道中取日志并发送消息到Kafka
	return
}

//将数据存放到chan中
func SendtoChan(topic, data string) {
	msg := &logDate{
		topic: topic,
		data:  data,
	}
	logDateChan <- msg
}

//从通道中取日志并发送消息到Kafka
func sendtoKafka() {
	for {
		select {
		case logDate := <-logDateChan:
			msg := &sarama.ProducerMessage{}
			msg.Topic = logDate.topic
			msg.Value = sarama.StringEncoder(logDate.data)
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				fmt.Println("send message failed,err:", err)
				return
			}
			fmt.Printf("pid:%d offset:%d\n", pid, offset)
			return
		default:
			time.Sleep(time.Millisecond * 50)
		}

	}
}
