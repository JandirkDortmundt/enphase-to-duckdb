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
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"

	// Import the tokenrefresher package
	// Assuming token_refresher.go is in a sub-directory named 'tokenrefresher'
	// and your go.mod defines your module path (e.g., module my/enphase_publisher)
	// The import path would then be "my/enphase_publisher/tokenrefresher"
	// You MUST adjust the import path below to match your actual module path and directory structure.
	"enphase/tokenrefresher" // <-- ADJUST THIS IMPORT PATH
)

// Configuration variables - Read from environment
var (
	envoyURL          string
	envoyCommCheckURL string
	kafkaBroker       string
	kafkaTopic        string
	fetchInterval     = 10 * time.Second

	// Owner token for data fetching
	envoyOwnerToken  string
	ownerTokenExpiry int64

	// Installer token for communication check
	envoyInstallerToken  string
	installerTokenExpiry int64

	// --- NEW: Configuration for enabling/disabling comm check ---
	enableCommCheck bool
	// --- END NEW ---
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

	// --- NEW: Read and parse ENABLE_COMM_CHECK ---
	enableCommCheckStr := os.Getenv("ENABLE_COMM_CHECK")
	if enableCommCheckStr != "" {
		parsedBool, err := strconv.ParseBool(enableCommCheckStr)
		if err != nil {
			log.Printf("Warning: Could not parse ENABLE_COMM_CHECK '%s' as boolean: %v. Defaulting to false.", enableCommCheckStr, err)
			enableCommCheck = false // Default to false if parsing fails
		} else {
			enableCommCheck = parsedBool
		}
	} else {
		log.Println("ENABLE_COMM_CHECK environment variable not set. Defaulting to false.")
		enableCommCheck = false // Default to false if variable is not set
	}
	// --- END NEW ---

	// --- NEW: Read and manage both Owner and Installer tokens ---

	// Read Owner token and expiry
	envoyOwnerToken = os.Getenv("ENVOY_TOKEN")
	ownerExpiryStr := os.Getenv("ENVOY_TOKEN_EXPIRY")

	// Read Installer token and expiry
	envoyInstallerToken = os.Getenv("ENVOY_INSTALLER_TOKEN")
	installerExpiryStr := os.Getenv("ENVOY_INSTALLER_TOKEN_EXPIRY")

	// Attempt initial refresh for Owner token if missing or expired
	if envoyOwnerToken == "" || ownerExpiryStr == "" {
		log.Println("Owner token or expiry not found. Attempting initial OWNER token refresh...")
		err := performOwnerTokenRefresh()
		if err != nil {
			log.Fatalf("Initial OWNER token refresh failed: %v", err)
		}
		// Reload .env after successful refresh
		godotenv.Load() // Ignore error, will fatal below if still missing
		envoyOwnerToken = os.Getenv("ENVOY_TOKEN")
		ownerExpiryStr = os.Getenv("ENVOY_TOKEN_EXPIRY")
		if envoyOwnerToken == "" || ownerExpiryStr == "" {
			log.Fatal("Owner token refresh succeeded, but ENVOY_TOKEN or ENVOY_TOKEN_EXPIRY still not found after reloading .env")
		}
	}

	// Attempt initial refresh for Installer token if missing or expired
	// Only attempt installer token refresh if comm check is enabled
	if enableCommCheck && (envoyInstallerToken == "" || installerExpiryStr == "") {
		log.Println("Installer token or expiry not found. Attempting initial INSTALLER token refresh...")
		err := performInstallerTokenRefresh()
		if err != nil {
			log.Fatalf("Initial INSTALLER token refresh failed: %v", err)
		}
		// Reload .env after successful refresh
		godotenv.Load() // Ignore error, will fatal below if still missing
		envoyInstallerToken = os.Getenv("ENVOY_INSTALLER_TOKEN")
		installerExpiryStr = os.Getenv("ENVOY_INSTALLER_TOKEN_EXPIRY")
		if envoyInstallerToken == "" || installerExpiryStr == "" {
			log.Fatal("Installer token refresh succeeded, but ENVOY_INSTALLER_TOKEN or ENVOY_INSTALLER_TOKEN_EXPIRY still not found after reloading .env")
		}
	}

	// Parse Owner expiry timestamp string to int64
	parsedOwnerExpiry, err := strconv.ParseInt(ownerExpiryStr, 10, 64)
	if err != nil {
		log.Printf("Warning: Could not parse ENVOY_TOKEN_EXPIRY '%s' as int64: %v. Owner token will be treated as expired.", ownerExpiryStr, err)
		ownerTokenExpiry = 0 // Treat as expired if parsing fails
	} else {
		ownerTokenExpiry = parsedOwnerExpiry
	}

	// Parse Installer expiry timestamp string to int64
	// Only parse if comm check is enabled and token/expiry were found/refreshed
	if enableCommCheck && installerExpiryStr != "" {
		parsedInstallerExpiry, err := strconv.ParseInt(installerExpiryStr, 10, 64)
		if err != nil {
			log.Printf("Warning: Could not parse ENVOY_INSTALLER_TOKEN_EXPIRY '%s' as int64: %v. Installer token will be treated as expired.", installerExpiryStr, err)
			installerTokenExpiry = 0 // Treat as expired if parsing fails
		} else {
			installerTokenExpiry = parsedInstallerExpiry
		}
	} else {
		// If comm check is disabled or token/expiry not found, treat installer token as expired/unavailable
		installerTokenExpiry = 0
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
	log.Printf("Current OWNER token expires at %s (Unix: %d)", time.Unix(ownerTokenExpiry, 0).UTC().Format(time.RFC3339), ownerTokenExpiry)
	log.Printf("Current INSTALLER token expires at %s (Unix: %d)", time.Unix(installerTokenExpiry, 0).UTC().Format(time.RFC3339), installerTokenExpiry)
	log.Printf("Envoy communication check is %s", map[bool]string{true: "ENABLED", false: "DISABLED"}[enableCommCheck])
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

	// Initial fetch to populate state (using the potentially newly refreshed OWNER token)
	log.Println("Performing initial fetch to populate state...")
	// --- MODIFIED: Call appropriate initial fetch function based on enableCommCheck ---
	if enableCommCheck {
		// If comm check is enabled, perform initial fetch via the trigger function
		fetchTriggerAndPublish(ctx, writer)
	} else {
		// If comm check is disabled, perform initial fetch by just fetching data
		fetchAndPopulateState(ctx) // This function only fetches data and populates state
	}
	// --- END MODIFIED ---

	log.Println("Initial state populated. Starting periodic fetching and publishing.")

	for {
		select {
		case <-ticker.C:
			// --- NEW: Check token expiries before each cycle ---
			// Refresh Owner token if needed
			if time.Now().Unix()+300 >= ownerTokenExpiry { // Check 5 minutes before expiry
				log.Println("Owner token is expired or nearing expiry. Attempting to refresh OWNER token...")
				err := performOwnerTokenRefresh()
				if err != nil {
					log.Printf("OWNER token refresh failed: %v. Continuing with potentially expired token.", err)
					// Decide if you want to stop or continue with the old token
				} else {
					// If refresh successful, reload .env to get the new token and expiry
					godotenv.Load() // Ignore error, will fatal below if still missing
					envoyOwnerToken = os.Getenv("ENVOY_TOKEN")
					ownerExpiryStr := os.Getenv("ENVOY_TOKEN_EXPIRY")
					if ownerExpiryStr != "" {
						parsedExpiry, err := strconv.ParseInt(ownerExpiryStr, 10, 64)
						if err == nil {
							ownerTokenExpiry = parsedExpiry
							log.Printf("Successfully refreshed OWNER token. New token expires at %s (Unix: %d)", time.Unix(ownerTokenExpiry, 0).UTC().Format(time.RFC3339), ownerTokenExpiry)
						} else {
							log.Printf("Warning: Could not parse new ENVOY_TOKEN_EXPIRY '%s' after refresh: %v. Owner token will be treated as expired.", ownerExpiryStr, err)
							ownerTokenExpiry = 0
						}
					} else {
						log.Println("Warning: ENVOY_TOKEN_EXPIRY not found in .env after OWNER refresh. Owner token will be treated as expired.")
						ownerTokenExpiry = 0
					}
				}
			}

			// Refresh Installer token if needed (only if comm check is enabled)
			if enableCommCheck && (time.Now().Unix()+300 >= installerTokenExpiry) { // Check 5 minutes before expiry
				log.Println("Installer token is expired or nearing expiry. Attempting to refresh INSTALLER token...")
				err := performInstallerTokenRefresh()
				if err != nil {
					log.Printf("INSTALLER token refresh failed: %v. Continuing with potentially expired token.", err)
					// Decide if you want to stop or continue with the old token
				} else {
					// If refresh successful, reload .env to get the new token and expiry
					godotenv.Load() // Ignore error, will fatal below if still missing
					envoyInstallerToken = os.Getenv("ENVOY_INSTALLER_TOKEN")
					installerExpiryStr := os.Getenv("ENVOY_INSTALLER_TOKEN_EXPIRY")
					if installerExpiryStr != "" {
						parsedExpiry, err := strconv.ParseInt(installerExpiryStr, 10, 64)
						if err == nil {
							installerTokenExpiry = parsedExpiry
							log.Printf("Successfully refreshed INSTALLER token. New token expires at %s (Unix: %d)", time.Unix(installerTokenExpiry, 0).UTC().Format(time.RFC3339), installerTokenExpiry)
						} else {
							log.Printf("Warning: Could not parse new ENVOY_INSTALLER_TOKEN_EXPIRY '%s' after refresh: %v. Installer token will be treated as expired.", installerExpiryStr, err)
							installerTokenExpiry = 0
						}
					} else {
						log.Println("Warning: ENVOY_INSTALLER_TOKEN_EXPIRY not found in .env after INSTALLER refresh. Installer token will be treated as expired.")
						installerTokenExpiry = 0
					}
				}
			}
			// --- END NEW ---

			// --- MODIFIED: Call appropriate fetch function based on enableCommCheck ---
			if enableCommCheck {
				fetchTriggerAndPublish(ctx, writer)
			} else {
				fetchAndPublishOnly(ctx, writer) // Call the new function that only fetches data
			}
			// --- END MODIFIED ---
		case <-ctx.Done():
			log.Println("Context cancelled, shutting down.")
			return
		}
	}
}

// performOwnerTokenRefresh calls the token refresher logic for the Owner token.
func performOwnerTokenRefresh() error {
	// Call the RefreshAndSaveOwnerToken function from the tokenrefresher package
	// ADJUST THE CALL BASED ON YOUR IMPORT PATH
	newToken, newExpiry, err := tokenrefresher.RefreshAndSaveOwnerToken()
	if err != nil {
		return fmt.Errorf("owner token refresh failed: %w", err)
	}

	// Update global variables (these will also be reloaded from .env by godotenv.Load in the caller)
	envoyOwnerToken = newToken
	ownerTokenExpiry = newExpiry

	return nil
}

// performInstallerTokenRefresh calls the token refresher logic for the Installer token.
func performInstallerTokenRefresh() error {
	// Call the RefreshAndSaveInstallerToken function from the tokenrefresher package
	// ADJUST THE CALL BASED ON YOUR IMPORT PATH
	newToken, newExpiry, err := tokenrefresher.RefreshAndSaveInstallerToken()
	if err != nil {
		return fmt.Errorf("installer token refresh failed: %w", err)
	}

	// Update global variables (these will also be reloaded from .env by godotenv.Load in the caller)
	envoyInstallerToken = newToken
	installerTokenExpiry = newExpiry

	return nil
}

// fetchAndPopulateState fetches data from Envoy and populates the lastReportDates map
// Used on initial startup when comm check is DISABLED. Uses the OWNER token.
func fetchAndPopulateState(ctx context.Context) {
	log.Printf("Attempting initial fetch from Envoy at %s (Comm Check Disabled)", envoyURL)

	req, err := http.NewRequestWithContext(ctx, "GET", envoyURL, nil)
	if err != nil {
		log.Printf("Error creating HTTP request for initial fetch: %v", err)
		return
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", "Bearer "+envoyOwnerToken) // Use the OWNER token

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

// fetchTriggerAndPublish calls the communication check endpoint, waits, then fetches and publishes new data.
// Uses the INSTALLER token for the comm check and the OWNER token for data fetching.
func fetchTriggerAndPublish(ctx context.Context, writer *kafka.Writer) {
	log.Printf("Attempting to trigger Envoy communication check at %s", envoyCommCheckURL)

	// Call the communication check endpoint using the INSTALLER token
	reqTrigger, err := http.NewRequestWithContext(ctx, "POST", envoyCommCheckURL, nil) // Assuming POST
	if err != nil {
		log.Printf("Error creating HTTP request for comm check: %v", err)
		// Continue to fetch anyway
	} else {
		reqTrigger.Header.Add("Authorization", "Bearer "+envoyInstallerToken) // Use the INSTALLER token

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

	// Existing logic to fetch from /api/v1/production/inverters using the OWNER token
	log.Printf("Attempting to fetch data from Envoy at %s after trigger", envoyURL)

	req, err := http.NewRequestWithContext(ctx, "GET", envoyURL, nil)
	if err != nil {
		log.Printf("Error creating HTTP request for inverter data: %v", err)
		return
	}

	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", "Bearer "+envoyOwnerToken) // Use the OWNER token

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

// --- NEW: Function to fetch data and publish without triggering comm check ---
func fetchAndPublishOnly(ctx context.Context, writer *kafka.Writer) {
	log.Printf("Attempting to fetch data from Envoy at %s (Comm Check Disabled)", envoyURL)

	req, err := http.NewRequestWithContext(ctx, "GET", envoyURL, nil)
	if err != nil {
		log.Printf("Error creating HTTP request for data fetch (no trigger): %v", err)
		return
	}

	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", "Bearer "+envoyOwnerToken) // Use the OWNER token

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error making HTTP request for data fetch (no trigger): %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Envoy returned non-OK status for data fetch (no trigger) from %s: %d %s", envoyURL, resp.StatusCode, resp.Status)
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading Envoy response body for data fetch (no trigger): %v", err)
		return
	}

	var inverters []Inverter
	err = json.Unmarshal(body, &inverters)
	if err != nil {
		log.Printf("Error unmarshalling Envoy response JSON for data fetch (no trigger): %v", err)
		return
	}

	log.Printf("Successfully fetched data for %d inverters (no trigger).", len(inverters))

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
		log.Printf("Attempting to publish %d new messages to Kafka (no trigger).", len(messages))
		writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		err = writer.WriteMessages(writeCtx, messages...)
		if err != nil {
			log.Printf("Error writing messages to Kafka (no trigger): %v", err)
		} else {
			log.Printf("Successfully published data for %d new panels to Kafka (no trigger).", len(messages))
		}
	} else {
		log.Println("No new inverter data to publish in this cycle (no trigger).")
	}
}

// --- END NEW ---
