package main

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"time"
)

const NSQADDR = "127.0.0.1:4150"

var logger Logger

// Producer 生产者
func Producer() {
	logger, err := New(true, "test")
	if err != nil {
		fmt.Println(err)
	}
	for {
		func() {
			producer, err := nsq.NewProducer(NSQADDR, nsq.NewConfig())
			if err != nil {
				fmt.Println(err)
			}
			defer producer.Stop()
			producer.SetLogger(NewNSQLogger(logger), nsq.LogLevelWarning)

			err = producer.Publish("test-topic", []byte(fmt.Sprintf("Hello World ")))
			if err != nil {
				logger.Error("fail to publish log message.", "error", err)
				return
			}
		}()
		time.Sleep(time.Second)
	}
}

//消费者1
func Consumer1() {
	consumer, err := nsq.NewConsumer("test", "channel-a", nsq.NewConfig())
	if err != nil {
		fmt.Println("NewConsumer fail:", err)
		panic(err)
	}
	consumer.SetLogger(NewNSQLogger(logger), nsq.LogLevelWarning)
	// 添加消息处理的具体实现(HandleMessage(msg *nsq.Message) error)
	consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		fmt.Println("Consumer1:", string(msg.Body))
		return nil
	}))

	if err := consumer.ConnectToNSQLookupd("127.0.0.1:4161"); err != nil {
		fmt.Println("ConnectToNSQLookupd fail:", err)
		panic(err)
	}
	<-consumer.StopChan
	logger.Info("nsq runner stopped.")
}

//消费者2
func Consumer2() {
	consumer, err := nsq.NewConsumer("test", "channel-a", nsq.NewConfig())
	if err != nil {
		fmt.Println("NewConsumer fail:", err)
		panic(err)
	}

	// 添加消息处理的具体实现
	consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		fmt.Println("Consumer2:", string(msg.Body))
		return nil
	}))
	if err := consumer.ConnectToNSQD("127.0.0.1:4161"); err != nil {
		fmt.Println("ConnectToNSQLookupd fail:", err)
		panic(err)
	}
}

//消费者3
func Consumer3() {
	consumer, err := nsq.NewConsumer("test", "channel-b", nsq.NewConfig())
	if err != nil {
		fmt.Println("NewConsumer fail:", err)
		panic(err)
	}

	// 添加消息处理的具体实现
	consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		fmt.Println("Consumer3:", string(msg.Body))
		return nil
	}))

	if err := consumer.ConnectToNSQLookupd("127.0.0.1:4161"); err != nil {
		fmt.Println("ConnectToNSQLookupd fail:", err)
		panic(err)
	}
}

func main() {
	Consumer1()
	Consumer2()
	Consumer3()
	Producer()
}

//consumer1和consumer2轮询输出（负载均衡）
//consumer3每次都输出
