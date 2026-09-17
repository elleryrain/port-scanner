package main

import (
	"fmt"
	"net"
	"time"
)

func main() {
	listener, err := net.Listen("tcp", "127.0.0.1:9000")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	fmt.Println("Слушаем порт 9000, но не отправляем данные")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}

		go func(conn net.Conn) {
			defer conn.Close()

			time.Sleep(time.Minute)
		}(conn)
	}
}