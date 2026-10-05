package handler

import (
	"encoding/csv"
	"encoding/json"
	"kafka_project/model"
	"net/http"
	"strings"
)

type Response struct {
	Total  int    `json:"total"`
	Queued int    `json:"inserted"` // rows handed to the worker pool
	Failed int    `json:"failed"`   // malformed / empty rows
	Note   string `json:"note"`
}

// UploadHandler accepts a CSV (multipart field "file"), parses each row into a
// model.User and pushes it into model.Jobs — the SAME channel the worker pool
// consumes. From there it follows the normal flow: worker -> Redis dedup ->
// Kafka -> consumer -> PostgreSQL. Watch the dashboard stats update live.
//
// CSV layout (people-100.csv):
//
//	Index, User Id, First Name, Last Name, Sex, Email, Phone, ...
//	  0       1         2          3        4     5      6
func UploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "use POST", http.StatusMethodNotAllowed)
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "file error: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // tolerate ragged rows
	rows, err := reader.ReadAll()
	if err != nil {
		http.Error(w, "csv parse error: "+err.Error(), http.StatusBadRequest)
		return
	}

	total := 0
	queued := 0
	failed := 0

	// Collect valid users first, then feed the pipeline in the background so the
	// HTTP response returns immediately (model.Jobs is unbuffered).
	var users []model.User

	for i, row := range rows {
		if i == 0 {
			continue // skip header
		}
		total++

		if len(row) < 6 {
			failed++
			continue
		}

		name := strings.TrimSpace(row[2] + " " + row[3])
		email := strings.TrimSpace(row[5])
		if email == "" {
			failed++
			continue
		}

		users = append(users, model.User{Name: name, Email: email})
		queued++
	}

	// Push into the worker pool without blocking the HTTP response.
	go func() {
		for _, u := range users {
			model.Jobs <- u
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Total:  total,
		Queued: queued,
		Failed: failed,
		Note:   "queued to worker pool — watch the dashboard",
	})
}
