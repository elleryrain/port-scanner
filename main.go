package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	workers           = 3
	maxPort           = 65535
	connectionTimeout = 500 * time.Millisecond
)

type ScanStats struct {
	mu sync.Mutex

	started   int
	active    int
	checked   int
	failed    int
	timeout   int
	canceled  int
	openPorts []int
}

func (s *ScanStats) Print(startedAt time.Time) {
	s.mu.Lock()

	started := s.started
	active := s.active
	checked := s.checked
	failed := s.failed
	timeout := s.timeout
	canceled := s.canceled
	open := len(s.openPorts)

	s.mu.Unlock()

	elapsed := time.Since(startedAt)
	percent := 100 * float64(checked) / float64(maxPort)
	speed := float64(checked) / elapsed.Seconds()

	fmt.Printf(
		" Время: %s | Проверено: %d/%d (%.1f%%) | "+
			"Открыто: %d | Ошибки: %d | Тайм-ауты: %d |"+
			"Отменено: %d | Активно: %d | Не начато: %d"+
			"Скорость: %.1f порт /c\n",
		elapsed.Round(time.Millisecond),
		checked,
		maxPort,
		percent,
		open,
		failed,
		timeout,
		canceled,
		active,
		maxPort-started,
		speed,
	)
}

func main() {

	host := "127.0.0.1"

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
	)

	defer stop()

	dialer := net.Dialer{
		Timeout: connectionTimeout,
	}

	var stats ScanStats

	progressCtx, stopProgress := context.WithCancel(
		context.Background(),
	)
	defer stopProgress()

	var progressWg sync.WaitGroup
	progressWg.Add(1)

	startedAt := time.Now()

	go func() {
		defer progressWg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				stats.Print(startedAt)
			case <-progressCtx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(workers)

	for start := 1; start <= workers; start++ {
		go func(firstPort int) {
			defer wg.Done()

			for port := firstPort; port <= maxPort; port += workers {
				if ctx.Err() != nil {
					return
				}
				stats.mu.Lock()
				stats.started++
				stats.active++
				stats.mu.Unlock()

				address := net.JoinHostPort(host, strconv.Itoa(port))

				conn, err := dialer.DialContext(ctx, "tcp", address)

				if err == nil {
					conn.Close()
				}

				canceled := errors.Is(err, context.Canceled)

				stats.mu.Lock()
				stats.active--

				switch {
				case canceled:
					stats.canceled++

				case err == nil:
					stats.checked++
					stats.openPorts = append(stats.openPorts, port)

				default:
					stats.checked++
					netErr, ok := err.(net.Error)
					if ok && netErr.Timeout() {
						stats.timeout++
					} else {
						stats.failed++
					}
				}
				stats.mu.Unlock()
				if canceled {
					return
				}
			}

		}(start)
	}

	wg.Wait()

	stopProgress()
	progressWg.Wait()

	if stats.checked == maxPort {
		fmt.Println("Сканирование завершено")
	} else {
		fmt.Println("Сканирование остановлено: ", ctx.Err())
	}

	stats.Print(startedAt)
	sort.Ints(stats.openPorts)

	fmt.Printf("Открытые TCP-порты (%d): %v\n", len(stats.openPorts), stats.openPorts)
}
