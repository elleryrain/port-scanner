package main

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"
)
const (
		workers = 100
		maxPort = 65535
		connectionTimeout = 500 * time.Millisecond
		scanTimeout = 20 * time.Second
)

func main() {

	host := "127.0.0.1"

	ctx, cancel := context.WithTimeout(
		context.Background(),
		scanTimeout,
	)
	
	defer cancel()

	dialer := net.Dialer {
		Timeout: connectionTimeout,
	}

	var (
		wg sync.WaitGroup
		mu sync.Mutex
		openPorts []int
	)
	wg.Add(workers)

	for start := 1; start <= workers; start++ {
		go func (firstPort int)  {
			defer wg.Done()

			for port := firstPort; port <= maxPort; port += workers {
				if ctx.Err() != nil {
					return
				}
				address := net.JoinHostPort(host, strconv.Itoa(port))

				conn, err := dialer.DialContext(ctx, "tcp", address)

				if err != nil {
					if ctx.Err() != nil {
						return
					}

					continue
				}

				conn.Close()
				mu.Lock()
				openPorts = append(openPorts, port)
				mu.Unlock()
			}
			
			
		}(start)
	}

	wg.Wait()

	sort.Ints(openPorts)

	if ctx.Err() != nil {
		fmt.Println("Сканирование остановлено: ", ctx.Err())

	} else {
		fmt.Println("Сканирование завершено")
	}

	fmt.Printf("Найдено открытых TCP-портов: %d\n", len(openPorts))
	for _, port := range openPorts {
		fmt.Println(port)
	}
}