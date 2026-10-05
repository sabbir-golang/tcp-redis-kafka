package handler

import (
	"context"
	"database/sql"
	"kafka_project/tcp"
	"net"
	"net/http"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

// Infra targets. These mirror the addresses already used in main.go and
// consumer.go — keep them in sync if you change ports.
const (
	healthRedisAddr   = "localhost:6379"
	healthKafkaBroker = "localhost:9092"
	healthPgConn      = "host=localhost user=postgres password=123456 dbname=student sslmode=disable"
)

const healthTimeout = 1 * time.Second

// HealthResponse matches the JSON shape expected by dashboard.html.
type HealthResponse struct {
	Redis    string `json:"redis"`
	Kafka    string `json:"kafka"`
	Postgres string `json:"postgres"`
	Tcp      string `json:"tcp"`
}

// Health handles GET /health.
func Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, HealthResponse{
		Redis:    checkRedis(),
		Kafka:    checkTCP(healthKafkaBroker), // a reachable broker port = online
		Postgres: checkPostgres(),
		Tcp:      checkTCPServer(),
	})
}

func checkRedis() string {
	ctx, cancel := context.WithTimeout(context.Background(), healthTimeout)
	defer cancel()

	c := redis.NewClient(&redis.Options{Addr: healthRedisAddr})
	defer c.Close()

	if err := c.Ping(ctx).Err(); err != nil {
		return "offline"
	}
	return "online"
}

func checkPostgres() string {
	db, err := sql.Open("postgres", healthPgConn)
	if err != nil {
		return "offline"
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), healthTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return "offline"
	}
	return "online"
}

// checkTCPServer reports the status of the in-process TCP server without
// dialing it — dialing would log "Client connected" / "read error" on every
// health poll. We just read the listener flag from the tcp package.
func checkTCPServer() string {
	if tcp.IsUp() {
		return "online"
	}
	return "offline"
}

// checkTCP reports "online" if a TCP connection to addr can be opened.
// Used for the Kafka broker. A quick connect+close is harmless.
func checkTCP(addr string) string {
	conn, err := net.DialTimeout("tcp", addr, healthTimeout)
	if err != nil {
		return "offline"
	}
	conn.Close()
	return "online"
}
