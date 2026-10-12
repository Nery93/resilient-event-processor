package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

func main() {
	// Set up signal handling to gracefully shut down the reader
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "raw-events",
		GroupID: "worker-group",
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("close reader: %v", err)
		}
	}()

	maxConcurrentWorkers := make(chan struct{}, 5) // limit concurrency to 5 goroutines
	group := new(errgroup.Group)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// context was canceled, exit gracefully
				break
			}
			log.Fatal(err)
		}

		maxConcurrentWorkers <- struct{}{} // acquire a slot
		msgCopy := msg
		group.Go(func() error {
			defer func() { <-maxConcurrentWorkers }() // release the slot
			fmt.Println(string(msgCopy.Value))
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		log.Fatal(err)
	}
}
