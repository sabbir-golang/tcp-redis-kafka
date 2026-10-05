package client

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
)

type User struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

func Worker(id int, jobs <-chan User, wg *sync.WaitGroup) {
	defer wg.Done()
	for usr := range jobs {
		data, _ := json.Marshal(usr)

		conn, _ := net.Dial("tcp", "localhost:9000")
		conn.Write(data)
		conn.Close()
		fmt.Println("Worker", id, "sent:", usr.Email)
	}
}

func main() {

	jobs := make(chan User, 100)

	var wg sync.WaitGroup
	file, err := os.Open("data/people-100.csv")
	if err != nil {
		log.Fatal(err)
		return
	}
	defer file.Close()
	reader := csv.NewReader(file)
	record, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
		return
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go Worker(i, jobs, &wg)
	}
	for i, row := range record {
		if i == 0 {
			continue
		}

		usr := User{
			Name:  row[1] + " " + row[2],
			Email: row[4],
		}
		jobs <- usr
	}
	close(jobs)
	wg.Wait()

}

// var wg sync.WaitGroup

// func workers(id int, jobs chan int) {
// 	defer wg.Done()
// 	for job := range jobs {

// 		fmt.Println("workers ", id, " processing job ", job)
// 	}
// }
// func main() {
// 	job := make(chan int)
// 	wg.Add(3)
// 	go workers(1, job)
// 	go workers(2, job)
// 	go workers(3, job)
// 	job <- 1
// 	job <- 2
// 	job <- 3
// 	job <- 4
// 	job <- 5
// 	job <- 6
// 	job <- 7
// 	close(job)
// 	wg.Wait()

// }
