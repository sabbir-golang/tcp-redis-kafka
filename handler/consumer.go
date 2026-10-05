package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type User struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

func InsertDb(db *sql.DB, batch []User) {
	if len(batch) == 0 {
		return
	}

	var value []interface{}
	var placeholder []string
	query := "INSERT INTO users(name,email) VALUES "
	for i, user := range batch {
		placeholder = append(placeholder, fmt.Sprintf("($%d,$%d)", i*2+1, i*2+2))
		value = append(value, user.Name, user.Email)
	}
	// ON CONFLICT DO NOTHING = if an email is already in the table, just skip
	// that ONE row instead of throwing away the whole busload. (Bug 1 fix)
	query += strings.Join(placeholder, ",") + " ON CONFLICT (email) DO NOTHING"

	res, err := db.Exec(query, value...)
	if err != nil {
		log.Println("Insert error:", err)
		return
	}

	// RowsAffected tells us how many were REALLY new. The rest were duplicates.
	count, _ := res.RowsAffected()
	AddInserted(count)                          // dashboard: count the whole batch (Bug 2 fix)
	IncDuplicateBy(int64(len(batch)) - count)   // the skipped ones were duplicates

	// The activity table only shows the latest 50, so we record just the last
	// one of the batch as a "sign of life" instead of looping 10,000 times.
	last := batch[len(batch)-1]
	RecordActivity(last.Name, last.Email, "inserted")

	fmt.Println("Batch inserted:", count, "of", len(batch))
}
func DBconsumer() {
	db, err := sql.Open("postgres", "host=localhost user=postgres password=123456 dbname=student sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "users",
		GroupID: "user-group",
	})

	fmt.Println("Consumer started...")

	// batchSize is how full the bus gets before we drive.
	// Postgres allows at most 65535 "$" placeholders per command. We use 2 per
	// row (name + email), so 5000 rows = 10000 placeholders — fast and safe.
	const batchSize = 5000
	var batch []User

	for {
		// Wait up to 1 second for the next message. If none comes in time,
		// ReadMessage returns an error and we use that moment to drive a
		// half-empty bus, so no kids are ever left at the stop. (Bug 3 fix)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		msg, err := reader.ReadMessage(ctx)
		cancel()

		if err != nil {
			// Timed out (or a read hiccup): flush whatever we have so far.
			InsertDb(db, batch)
			batch = nil
			continue
		}

		var user User
		if err := json.Unmarshal(msg.Value, &user); err != nil {
			fmt.Println("JSON error:", err)
			continue
		}

		batch = append(batch, user)
		if len(batch) >= batchSize {
			InsertDb(db, batch)
			batch = nil
		}
	}
}
