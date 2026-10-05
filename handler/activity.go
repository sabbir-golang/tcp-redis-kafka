package handler

import (
	"net/http"
	"sync"
	"time"
)

type Activity struct {
	Name   string `json:"name"`
	Email  string `json:"email"`
	Status string `json:"status"`
	Time   string `json:"time"`
}

var (
	activityMu    sync.Mutex
	activities    []Activity
	maxActivities = 50
)

func RecordActivity(name, email, status string) {
	activityMu.Lock()
	defer activityMu.Unlock()

	a := Activity{
		Name:   name,
		Email:  email,
		Status: status,
		Time:   time.Now().Format("2006-01-02 15:04:05"),
	}

	// Prepend newest, then trim to the cap.
	activities = append([]Activity{a}, activities...)
	if len(activities) > maxActivities {
		activities = activities[:maxActivities]
	}
}

func Activities(w http.ResponseWriter, r *http.Request) {
	activityMu.Lock()
	defer activityMu.Unlock()
	out := activities
	if out == nil {
		out = []Activity{}
	}
	writeJSON(w, out)
}
