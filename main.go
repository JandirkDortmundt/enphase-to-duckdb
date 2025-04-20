package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/segmentio/kafka-go" // Requires: go get github.com/segmentio/kafka-go
)

// Configuration variables - Read from environment
var (
	envoyURL      string
	kafkaBroker   string
	kafkaTopic    string
	fetchInterval = 5 * time.Minute // This can remain a constant or also come from env
)

// Structs to match the JSON response from the Envoy API
// These fields match the JSON keys you provided.
type Inverter struct {
	SerialNumber    string `json:"serialNumber"`   // Changed to string based on your JSON
	LastReportDate  int64  `json:"lastReportDate"` // Unix timestamp
	DevType         int    `json:"devType"`
	LastReportWatts int    `json:"lastReportWatts"`
	MaxReportWatts  int    `json:"maxReportWatts"`
}

// Struct to represent the data we'll send to Kafka
// This includes the essential data and a processing timestamp.
type PanelData struct {
	SerialNumber string    `json:"serialNumber"`
	Timestamp    time.Time `json:"timestamp"` // Timestamp when the data was fetched/processed
	Watts        int       `json:"watts"`
}

func init() {
	// This function runs before main()
	// Read configuration from environment variables
	envoyIP := os.Getenv("ENVOY_IP")
	if envoyIP == "" {
		log.Fatal("ENVOY_IP environment variable not set")
	}
	envoyURL = fmt.Sprintf("http://%s/api/v1/production/inverters", envoyIP)

	kafkaBroker = os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		log.Fatal("KAFKA_BROKER environment variable not set")
	}

	kafkaTopic = os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		log.Fatal("KAFKA_TOPIC environment variable not set")
	}

	// Optional: Read fetch interval from env
	// intervalStr := os.Getenv("FETCH_INTERVAL_MINUTES")
	// if intervalStr != "" {
	//     if minutes, err := strconv.Atoi(intervalStr); err == nil {
	//         fetchInterval = time.Duration(minutes) * time.Minute
	//     } else {
	//         log.Printf("Warning: Invalid FETCH_INTERVAL_MINUTES '%s', using default %s", intervalStr, fetchInterval)
	//     }
	// }

	log.Printf("Configuration loaded: EnvoyURL=%s, KafkaBroker=%s, KafkaTopic=%s", envoyURL, kafkaBroker, kafkaTopic)
}

func main() {
	// ... rest of your main function (same as before)
	// The variables envoyURL, kafkaBroker, kafkaTopic are now populated by init()
	log.SetOutput(os.Stdout)
	log.Println("Starting Enphase data collector...")

	ctx := context.Background()
	producer := &kafka.Producer{
		Addr:     kafka.TCP(kafkaBroker), // Uses the variable
		Topic:    kafkaTopic,             // Uses the variable
		Balancer: &kafka.LeastBytes{},
		Timeout:  10 * time.Second,
		Logger:   log.New(os.Stdout, "KAFKA ", log.LstdFlags),
	}
	// ... rest of main and fetchAndPublish (same as before)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("Failed to close Kafka producer: %v", err)
		}
		log.Println("Kafka producer closed.")
	}()

	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	fetchAndPublish(ctx, producer) // Initial fetch

	for {
		select {
		case <-ticker.C:
			fetchAndPublish(ctx, producer)
		case <-ctx.Done():
			log.Println("Context cancelled, shutting down.")
			return
		}
	}
}

// fetchAndPublish fetches data from Envoy and publishes to Kafka
func fetchAndPublish(ctx context.Context, producer *kafka.Producer) {
	log.Printf("Attempting to fetch data from Envoy at %s", envoyURL)
	resp, err := http.Get(envoyURL)
	if err != nil {
		log.Printf("Error making HTTP request to Envoy: %v", err)
		return // Stop processing this cycle on error
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Envoy returned non-OK status: %d %s", resp.StatusCode, resp.Status)
		return // Stop processing this cycle on error
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading Envoy response body: %v", err)
		return // Stop processing this cycle on error
	}

	var inverters []Inverter // The response is a JSON array of inverters
	err = json.Unmarshal(body, &inverters)
	if err != nil {
		log.Printf("Error unmarshalling Envoy response JSON: %v", err)
		return // Stop processing this cycle on error
	}

	log.Printf("Successfully fetched data for %d inverters.", len(inverters))

	// Process each inverter and publish to Kafka
	for _, inverter := range inverters {
		// Create the data structure for Kafka
		panelData := PanelData{
			SerialNumber: inverter.SerialNumber,
			Timestamp:    time.Now().UTC(), // Use UTC timestamp for consistency
			Watts:        inverter.LastReportWatts,
		}

		// Marshal the data to JSON for the Kafka message value
		panelDataJSON, err := json.Marshal(panelData)
		if err != nil {
			log.Printf("Error marshalling panel data for serial %s: %v", inverter.SerialNumber, err)
			continue // Skip this panel but continue with others
		}

		// Create the Kafka message
		msg := kafka.Message{
			Key:   []byte(panelData.SerialNumber), // Use serial number as the message key
			Value: panelDataJSON,
			Topic: kafkaTopic,
			Time:  panelData.Timestamp, // Use the timestamp from the data
		}

		// Publish message to Kafka
		// Use context with timeout for publishing
		writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // Timeout for writing a single message
		defer cancel()                                                               // Ensure cancel is called

		err = producer.WriteMessages(writeCtx, msg)
		if err != nil {
			log.Printf("Error writing message for panel %s to Kafka: %v", inverter.SerialNumber, err)
			// Depending on your error handling strategy, you might retry or log and continue
		} else {
			log.Printf("Successfully published data for panel %s to Kafka.", inverter.SerialNumber)
		}
	}
}
