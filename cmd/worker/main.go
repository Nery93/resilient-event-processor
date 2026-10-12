package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "raw-events",
		GroupID: "worker-group",
	})

	maxConcurrentWorkers  := make(chan struct{}, 5) // limit concurrency to 5 goroutines

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		maxConcurrentWorkers <- struct{}{} // acquire a slot
		go func(msg kafka.Message) {
			defer func() { <-maxConcurrentWorkers }() // release the slot
			fmt.Println(string(msg.Value))
		}(msg)

	}
}
