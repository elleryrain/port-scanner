package main

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)
const TIMEOUT = 500 * time.Millisecond
const PORTS_COUNT = 65535
func main() {

	host := "127.0.0.1"

	const workers = 100


	var wg sync.WaitGroup

	wg.Add(workers)

	for start := 1; start <= workers; start++ {
		go func (firstPort int)  {
			defer wg.Done()

			for port := firstPort; port <= PORTS_COUNT; port += workers {
				address := net.JoinHostPort(host, strconv.Itoa(port))

				conn, err := net.DialTimeout(
					"tcp",
					address,
					TIMEOUT,
				)
				if err != nil {
				fmt.Printf("Порт %d закрыт \n", port)
				continue
			}

			conn.Close()
			fmt.Printf("Порт %d открыт \n", port)
			}
			
			
		}(start)
	}
	
	wg.Wait()
	fmt.Println("Сканирование завершено")
}