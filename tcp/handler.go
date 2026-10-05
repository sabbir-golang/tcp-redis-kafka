package tcp

import (
	"encoding/json"
	"fmt"
	"kafka_project/model"
	"net"
)

var listener net.Listener

func MsHandle(conn net.Conn) {
	defer conn.Close()
	var user model.User
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	json.Unmarshal(buffer[:n], &user)
	if err != nil {
		fmt.Println("read error")
		return
	}
	model.Jobs <- user
}
func ConnTCP() {
	var err error
	l, err := net.Listen("tcp", ":9000")
	if err != nil {
		fmt.Println("Tcp connection failed")
		return
	}
	listener = l
	// defer l.Close()
}

// IsUp reports whether the TCP server is listening. Used by the dashboard
// /health endpoint so it doesn't have to dial (and spam) the listener.
func IsUp() bool {
	return listener != nil
}

func RcvTcp() {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("TCp recive failed")
			continue
		}
		fmt.Println("Client connected")
		go MsHandle(conn)
	}

}
