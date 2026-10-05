package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"kafka_project/model"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func Worker(id int, jobs <-chan model.User, rdb *redis.Client, writer *kafka.Writer) {
	WorkerStarted()       // dashboard: this worker is now active
	defer WorkerStopped() // dashboard: worker exited (channel closed)

	ctx := context.Background()
	for user := range jobs {
		start := time.Now() // dashboard: measure processing time
		IncProcessed()      // dashboard: a user entered the pipeline

		key := "user:" + user.Email
		val, err := rdb.Exists(ctx, key).Result()
		if err != nil {
			fmt.Println("Redis Error")
			continue
		}
		if val == 1 {
			fmt.Printf("Duplicate : %s\n", user.Email)
			IncDuplicate()                                     // dashboard: Redis caught a duplicate
			RecordActivity(user.Name, user.Email, "duplicate") // dashboard: recent activity
			continue
		}

		rdb.Set(ctx, key, "1", 0)
		data, err := json.Marshal(user)
		if err != nil {
			fmt.Println("Marshal error")
			continue
		}

		// Async writer: this returns immediately and the message is sent in the
		// background in big batches. We do NOT count it here — the real count
		// happens in the writer's Completion callback (see main.go).
		err = writer.WriteMessages(
			ctx,
			kafka.Message{
				Key:   []byte(user.Email),
				Value: data,
			},
		)
		if err != nil {
			fmt.Println("Kafka Error:", err)
			RecordActivity(user.Name, user.Email, "failed") // dashboard: produce failed
			continue
		}

		SetProcessingMs(time.Since(start).Milliseconds()) // dashboard: latest processing time
		// NOTE: no fmt.Printf per message — printing to the terminal millions of
		// times is slow and would cap your speed. Watch the dashboard instead.
	}
}
