package main

import (
	"fmt"
	"kafka_project/handler"
	"kafka_project/model"
	"kafka_project/tcp"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func main() {
	// handler.DbConnect()
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "users",
	})

	defer writer.Close()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	tcp.ConnTCP()
	fmt.Println("Server running on: 8080")

	for i := 0; i < 3; i++ {
		go handler.Worker(i, model.Jobs, rdb, writer)
	}
	tcp.RcvTcp()
}
