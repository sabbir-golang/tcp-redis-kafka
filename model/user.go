package model

type User struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

var Jobs = make(chan User)
