package handler

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Thread-safe pipeline counters.
//
// Every field is read/written only through sync/atomic, so they are safe to
// update concurrently from all workers and the Kafka consumer goroutine.
// These live in package handler because Worker and DBconsumer are already in
// this package and can call the Inc* helpers directly.
// ---------------------------------------------------------------------------
var (
	processed    int64 // users that entered the worker pool
	inserted     int64 // rows actually inserted into PostgreSQL
	duplicate    int64 // duplicates caught by Redis or the DB ON CONFLICT
	kafkaSent    int64 // messages successfully produced to Kafka
	activeWorker int64 // workers currently alive
	processingMs int64 // most recent per-message processing time (ms)

	startNano      int64 // time the FIRST record was processed (UnixNano)
	lastInsertNano int64 // time the LAST row was inserted (UnixNano)
	kafkaPerSec    int64 // Kafka messages sent in the last second
	insertPerSec   int64 // rows inserted in the last second
)

// Increment helpers — call these from the existing flow.
func IncProcessed() {
	atomic.AddInt64(&processed, 1)
	// Remember the moment the very first record started (only the first wins).
	if atomic.LoadInt64(&startNano) == 0 {
		atomic.CompareAndSwapInt64(&startNano, 0, time.Now().UnixNano())
	}
}
func IncInserted() { atomic.AddInt64(&inserted, 1) }
func AddInserted(n int64) {
	atomic.AddInt64(&inserted, n) // count a whole batch at once
	if n > 0 {
		atomic.StoreInt64(&lastInsertNano, time.Now().UnixNano()) // newest insert wins
	}
}
func IncDuplicateBy(n int64) { atomic.AddInt64(&duplicate, n) } // count skipped duplicates in a batch
func IncDuplicate()            { atomic.AddInt64(&duplicate, 1) }
func IncKafkaSent()            { atomic.AddInt64(&kafkaSent, 1) }
func AddKafkaSent(n int64)     { atomic.AddInt64(&kafkaSent, n) } // count a whole Kafka batch at once
func WorkerStarted()           { atomic.AddInt64(&activeWorker, 1) }
func WorkerStopped()           { atomic.AddInt64(&activeWorker, -1) }
func SetProcessingMs(ms int64) { atomic.StoreInt64(&processingMs, ms) }

// StartRateMeter runs forever in the background. Once a second it measures how
// many messages went to Kafka and how many rows were inserted in that second,
// giving us a live "per second" speed. Call it once from main as a goroutine.
func StartRateMeter() {
	var prevKafka, prevInserted int64
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		k := atomic.LoadInt64(&kafkaSent)
		i := atomic.LoadInt64(&inserted)
		atomic.StoreInt64(&kafkaPerSec, k-prevKafka)
		atomic.StoreInt64(&insertPerSec, i-prevInserted)
		prevKafka = k
		prevInserted = i
	}
}

// totalProcessingMs = time from the first processed record to the last insert.
func totalProcessingMs() int64 {
	s := atomic.LoadInt64(&startNano)
	l := atomic.LoadInt64(&lastInsertNano)
	if s == 0 || l <= s {
		return 0
	}
	return (l - s) / 1_000_000 // nanoseconds -> milliseconds
}

// StatsResponse matches the JSON shape expected by dashboard.html.
type StatsResponse struct {
	Processed    int64 `json:"processed"`
	Inserted     int64 `json:"inserted"`
	Duplicate    int64 `json:"duplicate"`
	KafkaSent    int64 `json:"kafka_sent"`
	Workers      int64 `json:"workers"`
	ProcessingMs int64 `json:"processing_ms"` // latest single-message time
	TotalMs      int64 `json:"total_ms"`      // total real processing time
	KafkaPerSec  int64 `json:"kafka_per_sec"` // Kafka messages / second
	InsertPerSec int64 `json:"insert_per_sec"`// Postgres inserts / second
}

// Stats handles GET /stats.
func Stats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, StatsResponse{
		Processed:    atomic.LoadInt64(&processed),
		Inserted:     atomic.LoadInt64(&inserted),
		Duplicate:    atomic.LoadInt64(&duplicate),
		KafkaSent:    atomic.LoadInt64(&kafkaSent),
		Workers:      atomic.LoadInt64(&activeWorker),
		ProcessingMs: atomic.LoadInt64(&processingMs),
		TotalMs:      totalProcessingMs(),
		KafkaPerSec:  atomic.LoadInt64(&kafkaPerSec),
		InsertPerSec: atomic.LoadInt64(&insertPerSec),
	})
}

// writeJSON is the shared JSON responder used by all dashboard endpoints.
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, "encode error", http.StatusInternalServerError)
	}
}
