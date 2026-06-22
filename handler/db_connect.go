package handler

import (
	"database/sql"
	"fmt"
)

var Db *sql.DB

func DbConnect() {
	connStr := "user=postgres password=123456 dbname=student sslmode=disable"
	Db, _ = sql.Open("postgres", connStr)
	// if err != nil {
	// 	fmt.Println("DB connection failed")
	// 	return
	// }
	fmt.Println("Db connected")
}
func UserAdd(id int, name string, email string, phone string) {
	sqlStatement := `insert into users(id , name, email, phone) values($1,$2,$3,$4)`
	_, err := Db.Exec(sqlStatement, id, name, email, phone)
	if err != nil {
		fmt.Println("Execute failed")
		return
	}
	fmt.Println("User Added")
}
