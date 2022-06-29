package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var messages = []string{"A", "B", "C"}

type Message struct {
	Timestamp time.Time
	Content   string
}

func (m Message) String() string {
	return fmt.Sprintf("[%v]: %s", m.Timestamp, m.Content)
}

func (m Message) Equal(other Message) bool {
	return m.Content == other.Content
}

func produce(q chan<- Message, freq time.Duration) {
	ticker := time.NewTicker(freq)
	for {
		<-ticker.C
		i := rand.Intn(len(messages))
		message := Message{Timestamp: time.Now(), Content: messages[i]}
		q <- message
		log.Println("produced", message)
	}
}

func consume(q <-chan Message) {
	for {
		message := <-q
		log.Println("consumed", message)
	}
}

func dedupe(source <-chan Message, frame time.Duration) chan Message {
	sink := make(chan Message)
	go func(s chan Message, t *time.Ticker) {
		buffer := make(map[string]Message, 0)
		for {
			select {
			case <-t.C:
				for _, m := range buffer {
					sink <- m
				}
				buffer = make(map[string]Message, 0)
			case m := <-source:
				buffer[m.Content] = m
			}
		}
	}(sink, time.NewTicker(frame))
	return sink
}

func main() {
	queue := make(chan Message)
	deduped := dedupe(queue, time.Second*5)
	go consume(deduped)
	go produce(queue, time.Second)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
