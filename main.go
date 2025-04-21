package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv" // Needed to parse expiry timestamp from string
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	// Import the new token_refresher package
	// Assuming token_refresher.go is in the same module or a sub-package
	// If in the same directory and same module, you can import it directly
	// If in a sub-directory like ./tokenrefresher, import as "your_module_path/tokenrefresher"
	// For simplicity, let's assume it's in the same package for now.
	// You might need to adjust the import path based on your go.mod and file structure.
	// Example if in the same directory/module:
	// "." // This assumes token_refresher.go is in the same directory and package main
	// However, it's better practice to put it in a separate package.
	// Let's assume you create a package named 'tokenrefresher' in a sub-directory.
	// You would need to change 'package main' to 'package tokenrefresher' in token_refresher.go
	// And your go.mod would define your module path, e.g., module my/enphase_publisher
	// Then the import would be: "my/enphase_publisher/tokenrefresher"
	// For now, let's assume it's in the same package for simplicity, but be aware of best practices.
	// If in the same package, the functions are directly available.
	// If in a different package, you'd call tokenrefresher.RefreshAndSaveToken
)

// Configuration variables - Read from environment
var (
	envoyURL          string
	envoyCommCheckURL string
	kafkaBroker       string
	kafkaTopic        string
	fetchInterval     = 10 * time.Second
	envoyToken        string // Variable to hold the current token
	tokenExpiry       int64  // Unix timestamp for token expiry
)

// State variable to store the last reported date for each inverter
var lastReportDates = make(map[string]int64)

// Structs (same as before)
type Inverter struct {
	SerialNumber    string `json:"serialNumber"`
	LastReportDate  int64  `json:"lastReportDate"`
	DevType         int    `json:"devType"`
	LastReportWatts int    `json:"lastReportWatts"`
	MaxReportWatts  int    `json:"maxReportWatts"`
}

// Struct for Kafka message value
type PanelData struct {
	SerialNumber string    `json:"serialNumber"`
	Timestamp    time.Time `json:"timestamp"`
	Watts        int       `json:"watts"`
}

func init() {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		log.Printf("Warning: Error loading .env file in main init: %v (This is okay if using system environment variables)", err)
	}

	// Read configuration from environment variables
	envoyIP := os.Getenv("ENVOY_IP")
	if envoyIP == "" {
		log.Fatal("ENVOY_IP environment variable not set")
	}
	envoyURL = fmt.Sprintf("https://%s/api/v1/production/inverters", envoyIP)
	envoyCommCheckURL = fmt.Sprintf("https://%s/installer/pcu_comm_check", envoyIP)

	kafkaBroker = os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		log.Fatal("KAFKA_BROKER environment variable not set")
	}

	kafkaTopic = os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		log.Fatal("KAFKA_TOPIC environment variable not set")
	}

	// Read the Envoy token and expiry from environment variables
	envoyToken = os.Getenv("ENVOY_TOKEN")
	expiryStr := os.Getenv("ENVOY_TOKEN_EXPIRY")

	// --- NEW: Handle missing or expired token on startup ---
	if envoyToken == "" || expiryStr == "" {
		log.Println("ENVOY_TOKEN or ENVOY_TOKEN_EXPIRY not found. Attempting initial token refresh...")
		// Attempt initial refresh if token or expiry is missing
		err := performTokenRefresh() // Call the refresh function
		if err != nil {
			log.Fatalf("Initial token refresh failed: %v", err)
		}
		// After successful refresh, the .env is updated. Reload it to get the new values.
		err = godotenv.Load()
		if err != nil {
			log.Fatalf("Failed to reload .env after initial refresh: %v", err)
		}
		// Read the newly refreshed token and expiry
		envoyToken = os.Getenv("ENVOY_TOKEN")
		expiryStr = os.Getenv("ENVOY_TOKEN_EXPIRY")
		if envoyToken == "" || expiryStr == "" {
			log.Fatal("Token refresh succeeded, but ENVOY_TOKEN or ENVOY_TOKEN_EXPIRY still not found after reloading .env")
		}
	}

	// Parse the expiry timestamp string to int64
	parsedExpiry, err := strconv.ParseInt(expiryStr, 10, 64)
	if err != nil {
		log.Printf("Warning: Could not parse ENVOY_TOKEN_EXPIRY '%s' as int64: %v. Token will be treated as expired.", expiryStr, err)
		tokenExpiry = 0 // Treat as expired if parsing fails
	} else {
		tokenExpiry = parsedExpiry
	}
	// --- END NEW ---

	// Optional: Read fetch interval from env
	// intervalStr := os.Getenv("FETCH_INTERVAL_SECONDS")
	// if intervalStr != "" {
	//     if seconds, err := strconv.Atoi(intervalStr); err == nil {
	//         fetchInterval = time.Duration(seconds) * time.Second
	//     } else {
	//         log.Printf("Warning: Invalid FETCH_INTERVAL_SECONDS '%s', using default %s", intervalStr, fetchInterval)
	//     }
	// }

	log.Printf("Configuration loaded: EnvoyURL=%s, CommCheckURL=%s, KafkaBroker=%s, KafkaTopic=%s, FetchInterval=%s",
		envoyURL, envoyCommCheckURL, kafkaBroker, kafkaTopic, fetchInterval)
	log.Printf("Current token expires at %s (Unix: %d)", time.Unix(tokenExpiry, 0).UTC().Format(time.RFC3339), tokenExpiry)
}

func main() {
	log.SetOutput(os.Stdout)
	log.Println("Starting Enphase data collector...")

	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        kafkaTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		Logger:       log.New(os.Stdout, "KAFKA ", log.LstdFlags),
		BatchTimeout: 5 * time.Second,
		BatchSize:    100,
	}

	defer func() {
		log.Println("Closing Kafka writer...")
		if err := writer.Close(); err != nil {
			log.Fatalf("Failed to close Kafka writer: %v", err)
		}
		log.Println("Kafka writer closed.")
	}()

	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initial fetch to populate state (using the potentially newly refreshed token)
	log.Println("Performing initial fetch to populate state...")
	fetchAndPopulateState(ctx)

	log.Println("Initial state populated. Starting periodic fetching and publishing.")

	for {
		select {
		case <-ticker.C:
			// --- NEW: Check token expiry before each cycle ---
			// Refresh if less than 5 minutes until expiry (or already expired)
			if time.Now().Unix()+300 >= tokenExpiry { // Check 5 minutes before expiry
				log.Println("Token is expired or nearing expiry. Attempting to refresh token...")
				err := performTokenRefresh() // Call the refresh function
				if err != nil {
					log.Printf("Token refresh failed: %v. Continuing with potentially expired token.", err)
					// Decide if you want to stop or continue with the old token
				} else {
					// If refresh successful, reload .env to get the new token and expiry
					err = godotenv.Load()
					if err != nil {
						log.Printf("Failed to reload .env after refresh: %v. Continuing with old token.", err)
					} else {
						envoyToken = os.Getenv("ENVOY_TOKEN")
						expiryStr := os.Getenv("ENVOY_TOKEN_EXPIRY")
						if expiryStr != "" {
							parsedExpiry, err := strconv.ParseInt(expiryStr, 10, 64)
							if err == nil {
								tokenExpiry = parsedExpiry
								log.Printf("Successfully refreshed token. New token expires at %s (Unix: %d)", time.Unix(tokenExpiry, 0).UTC().Format(time.RFC3339), tokenExpiry)
							} else {
								log.Printf("Warning: Could not parse new ENVOY_TOKEN_EXPIRY '%s' after refresh: %v. Token will be treated as expired.", expiryStr, err)
								tokenExpiry = 0 // Treat as expired
							}
						} else {
							log.Println("Warning: ENVOY_TOKEN_EXPIRY not found in .env after refresh. Token will be treated as expired.")
							tokenExpiry = 0 // Treat as expired
						}
					}
				}
			}
			// --- END NEW ---

			// Proceed with fetching and publishing using the current (potentially refreshed) token
			fetchTriggerAndPublish(ctx, writer)
		case <-ctx.Done():
			log.Println("Context cancelled, shutting down.")
			return
		}
	}
}

// performTokenRefresh calls the token refresher logic and updates global variables.
// This helper function is called from main and init.
func performTokenRefresh() error {
	// Call the RefreshAndSaveToken function from token_refresher.go
	// Assuming RefreshAndSaveToken is in the same package or imported correctly
	// If in a separate package 'tokenrefresher', call:
	// newToken, newExpiry, err := tokenrefresher.RefreshAndSaveToken()
	// For simplicity, assuming same package for now:
	newToken, newExpiry, err := RefreshAndSaveToken() // Call the function from token_refresher.go
	if err != nil {
		return fmt.Errorf("token refresh failed: %w", err)
	}

	// Update global variables (these will also be reloaded from .env by godotenv.Load in the caller)
	envoyToken = newToken
	tokenExpiry = newExpiry

	return nil
}

// fetchAndPopulateState fetches data from Envoy and populates the lastReportDates map
// Used on initial startup.
func fetchAndPopulateState(ctx context.Context) {
	log.Printf("Attempting initial fetch from Envoy at %s", envoyURL)

	// --- MODIFIED: Declare req and resp using := ---
	req, err := http.NewRequestWithContext(ctx, "GET", envoyURL, nil)
	if err != nil {
		log.Printf("Error creating HTTP request for initial fetch: %v", err)
		return
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", "Bearer "+envoyToken) // Use the current token

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error making HTTP request for initial fetch: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Envoy returned non-OK status for initial fetch from %s: %d %s", envoyURL, resp.StatusCode, resp.Status)
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading Envoy response body for initial fetch: %v", err)
		return
	}

	var inverters []Inverter
	err = json.Unmarshal(body, &inverters)
	if err != nil {
		log.Printf("Error unmarshalling Envoy response JSON for initial fetch: %v", err)
		return
	}

	log.Printf("Successfully fetched data for %d inverters during initial fetch.", len(inverters))

	// Populate the state map
	for _, inverter := range inverters {
		lastReportDates[inverter.SerialNumber] = inverter.LastReportDate
	}
	log.Printf("Initial state populated for %d inverters.", len(lastReportDates))
}

// fetchTriggerAndPublish calls the communication check endpoint, waits, then fetches and publishes new data
func fetchTriggerAndPublish(ctx context.Context, writer *kafka.Writer) {
	log.Printf("Attempting to trigger Envoy communication check at %s", envoyCommCheckURL)

	// Call the communication check endpoint
	reqTrigger, err := http.NewRequestWithContext(ctx, "POST", envoyCommCheckURL, nil) // Assuming POST
	if err != nil {
		log.Printf("Error creating HTTP request for comm check: %v", err)
		// Continue to fetch anyway
	} else {
		reqTrigger.Header.Add("Authorization", "Bearer "+envoyToken) // Use the current token

		clientTrigger := &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}

		respTrigger, err := clientTrigger.Do(reqTrigger)
		if err != nil {
			log.Printf("Error making HTTP request for comm check: %v", err)
			// Continue to fetch anyway
		} else {
			defer respTrigger.Body.Close()
			if respTrigger.StatusCode != http.StatusOK {
				log.Printf("Envoy comm check returned non-OK status from %s: %d %s", envoyCommCheckURL, respTrigger.StatusCode, respTrigger.Status)
				// Continue to fetch anyway
			} else {
				log.Println("Successfully triggered Envoy communication check.")
				// Optional: Read and log response body from trigger if it contains useful info
				// bodyTrigger, _ := io.ReadAll(respTrigger.Body)
				// log.Printf("Comm check response: %s", string(bodyTrigger))
			}
		}
	}

	// Wait briefly for the update to happen
	time.Sleep(2 * time.Second)
	log.Println("Waiting briefly after triggering communication check.")

	// Existing logic to fetch from /api/v1/production/inverters
	log.Printf("Attempting to fetch data from Envoy at %s after trigger", envoyURL)

	// --- MODIFIED: Declare req and resp using := ---
	req, err := http.NewRequestWithContext(ctx, "GET", envoyURL, nil)
	if err != nil {
		log.Printf("Error creating HTTP request for inverter data: %v", err)
		return
	}

	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", "Bearer "+envoyToken) // Use the current token

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error making HTTP request for inverter data: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Envoy returned non-OK status for inverter data from %s: %d %s", envoyURL, resp.StatusCode, resp.Status)
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading Envoy response body for inverter data: %v", err)
		return
	}

	// log.Printf("Received raw JSON from Envoy:\n%s", string(body)) // Commented out

	var inverters []Inverter
	err = json.Unmarshal(body, &inverters)
	if err != nil {
		log.Printf("Error unmarshalling Envoy response JSON for inverter data: %v", err)
		return
	}

	log.Printf("Successfully fetched data for %d inverters after trigger.", len(inverters))

	var messages []kafka.Message
	newReportsCount := 0

	for _, inverter := range inverters {
		lastSeenDate, exists := lastReportDates[inverter.SerialNumber]

		if !exists || inverter.LastReportDate > lastSeenDate {
			log.Printf("New data for panel %s (Current Date: %d, Last Seen: %d)",
				inverter.SerialNumber, inverter.LastReportDate, lastSeenDate)

			panelData := PanelData{
				SerialNumber: inverter.SerialNumber,
				Timestamp:    time.Now().UTC(),
				Watts:        inverter.LastReportWatts,
			}

			panelDataJSON, err := json.Marshal(panelData)
			if err != nil {
				log.Printf("Error marshalling panel data for serial %s: %v", inverter.SerialNumber, err)
				continue
			}

			msg := kafka.Message{
				Key:   []byte(panelData.SerialNumber),
				Value: panelDataJSON,
				Time:  panelData.Timestamp,
			}

			messages = append(messages, msg)
			newReportsCount++

			lastReportDates[inverter.SerialNumber] = inverter.LastReportDate
		} // else { log.Printf("No new data for panel %s ...") }
	}

	if len(messages) > 0 {
		log.Printf("Attempting to publish %d new messages to Kafka after trigger.", len(messages))
		writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		err = writer.WriteMessages(writeCtx, messages...)
		if err != nil {
			log.Printf("Error writing messages to Kafka after trigger: %v", err)
		} else {
			log.Printf("Successfully published data for %d new panels to Kafka after trigger.", len(messages))
		}
	} else {
		log.Println("No new inverter data to publish in this cycle after trigger.")
	}
}
