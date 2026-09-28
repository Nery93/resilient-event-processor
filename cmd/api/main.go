package main

import (
	"log"
	"net/http"
	"resilient-event-processor/internal/adapters/httpapi"
	"resilient-event-processor/internal/adapters/kafka"
)

func main() {

	producer := kafka.NewProducer([]string{"localhost:9092"}, "raw-events")
	handler := httpapi.NewHandler(producer)

	http.HandleFunc("/events", handler.PublishEvent)

	log.Println("Server starting on :8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
