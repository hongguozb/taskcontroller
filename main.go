package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"strconv"
)

var (
	addr     = flag.String("a", "127.0.0.1:9092", "broker addresses")
	topics   = flag.String("t", "", "topic split with comma ,")
	url      = flag.String("url", "", "url for dipatch")
	bind     = flag.String("bind", "0.0.0.0:9090", "httpserver for test send kafka ,bind addr and port")
	consumer sarama.Consumer
)

func init() {
	//进行kafka连接初始化的工作,从环境变量读取kafka连接配置
	flag.Parse()
	var err error
	consumer, err = sarama.NewConsumer(strings.Split(*addr, ","), nil)
	if err != nil {
		panic(err)
	}
}

func main() {
	defer func() {
		fmt.Println("begin defer")
		err := consumer.Close()
		if err != nil {
			fmt.Println(err)
		}
	}()

	//启动一个http server 用于接收数据发送到kafka进行数据测试
	go func() {
		http.HandleFunc("/sendtokafka", func(w http.ResponseWriter, r *http.Request) {
			body, _ := ioutil.ReadAll(r.Body)
			body_str := string(body)
			fmt.Println(body_str)

			producer, err := sarama.NewSyncProducer(strings.Split(*addr, ","), nil)
			if err != nil {
				log.Fatalln(err)
			}
			defer func() {
				if err := producer.Close(); err != nil {
					log.Fatalln(err)
				}
			}()

			for _, topic := range strings.Split(*topics, ",") {
				msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(body_str)}
				partition, offset, err := producer.SendMessage(msg)
				if err != nil {
					log.Printf("FAILED to send message: %s\n", err)
				} else {
					log.Printf("message sent to partition %d at offset %d\n", partition, offset)
				}
			}
		})
		log.Fatal(http.ListenAndServe(*bind, nil))
	}()

	ch := make(chan bool)

	//每一个topic启动一个协程处理
	for _, topic := range strings.Split(*topics, ",") {
		go func(tp string) {
			fmt.Println("topic:"+tp)
			partitions, _ := consumer.Partitions(tp)
			//每一个分区启动一个协程处理
			for _, partition := range partitions {
				fmt.Println("partition:"+strconv.Itoa(int(partition)))
				go func(par int32) {
					partitionConsumer, err := consumer.ConsumePartition(tp, par, sarama.OffsetNewest)
					if err != nil {
						panic(err)
					}
					consumed := 0
					defer func() {
						log.Printf("Consumed [%d]", consumed)
						if err := partitionConsumer.Close(); err != nil {
							log.Fatalln(err)
						}
					}()

					for {
						select {
						case msg := <-partitionConsumer.Messages():
							log.Printf("Consumed message  [%s]\n", string(msg.Value))
							go func(date []byte) {
								body := bytes.NewReader(date)
								resp, err := http.Post(*url, "application/json", body)
								if err != nil {
									fmt.Println(err)
								}
								resbody, err := ioutil.ReadAll(resp.Body)
								if err != nil {
									fmt.Println(err)
								}
								fmt.Println(string(resbody))
								defer resp.Body.Close()
							}(msg.Value)
							consumed++
						}
					}
				}(partition)
			}
		}(topic)
	}
	<-ch
}
