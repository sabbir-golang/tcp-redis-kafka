package main

import (
	"fmt"
	"kafka_project/handler"
	"kafka_project/model"
	"kafka_project/tcp"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func startDashboard() {
	http.HandleFunc("/", handler.Dashboard)
	http.HandleFunc("/stats", handler.Stats)
	http.HandleFunc("/activities", handler.Activities)
	http.HandleFunc("/health", handler.Health)
	http.HandleFunc("/upload", handler.UploadHandler)

	fmt.Println("Dashboard running on: http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("HTTP server error:", err)
	}
}

func main() {

	writer := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        "users",
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    1000,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        true,
		Completion: func(msgs []kafka.Message, err error) {

			if err != nil {
				fmt.Println("Kafka delivery error:", err)
				return
			}
			handler.AddKafkaSent(int64(len(msgs)))
		},
	}

	defer writer.Close()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	go handler.DBconsumer()
	tcp.ConnTCP()
	fmt.Println("Server running on: 9000")

	for i := 0; i < 20; i++ {
		go handler.Worker(i, model.Jobs, rdb, writer)
	}

	go startDashboard()
	go handler.StartRateMeter()

	tcp.RcvTcp()
}
