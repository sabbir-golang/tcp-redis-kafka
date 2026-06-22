package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"kafka_project/model"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func Worker(id int, jobs <-chan model.User, rdb *redis.Client, writer *kafka.Writer) {
	ctx := context.Background()
	for user := range jobs {
		key := "user:" + user.Email
		val, err := rdb.Exists(ctx, key).Result()
		if err != nil {
			fmt.Println("Redis Error")
			continue
		}
		if val == 1 {
			fmt.Printf("Duplicate : %s\n", user.Email)
			continue
		}

		rdb.Set(ctx, key, "1", 0)
		fmt.Printf("worker: %d\nName: %s\nEmail: %s\n", id, user.Name, user.Email)
		data, err := json.Marshal(user)
		if err != nil {
			fmt.Println("Marshal error")
			continue
		}
		err = writer.WriteMessages(
			ctx,
			kafka.Message{
				Key:   []byte(user.Email),
				Value: data,
			},
		)
		if err != nil {
			fmt.Println("Kafka Error:", err)
			continue
		}

		fmt.Printf(
			"Worker %d -> Sent To Kafka: %s\n",
			id,
			user.Email,
		)
		// fmt.Printf("worker %d proccessing %s \n", id, msg.Data)
	}
}
