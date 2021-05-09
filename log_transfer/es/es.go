package es

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/olivere/elastic"
)

type LogData struct {
	Topic string `json:"topic"`
	Data  string `json:"data"`
}

var (
	client  *elastic.Client
	logchan chan *LogData
)

//初始化ES，准备接收Kafk发来的数据
//初始化ES连接
func Init(adder string, maxSize, nums int) (err error) {
	if !strings.HasPrefix(adder, "http://") {
		adder = "http://" + adder
	}
	client, err = elastic.NewClient(elastic.SetURL(adder))
	if err != nil {
		return
	}
	fmt.Println("connect to ES success")
	logchan = make(chan *LogData, maxSize)
	for i := 0; i < nums; i++ {
		go SendToES()
	}
	return
}

//先从Kafka接收到的数据放到通道内
func SendToChan(msg *LogData) {

	logchan <- msg
}

//发送数据到ES
func SendToES() {
	for {
		select {
		case msg := <-logchan:
			//链式操作
			put1, err := client.Index().Index(msg.Topic).Type("xxx").BodyJson(msg).Do(context.Background())
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf("Iddexed student %s to index %s,type %s\n", put1.Id, put1.Index, put1.Type)
		default:
			time.Sleep(time.Second)
		}

	}

}
