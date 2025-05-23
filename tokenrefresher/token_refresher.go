package tokenrefresher // Use a separate package for better organization

import (
	//"bytes" // Needed for sending JSON body in POST requests
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/cookiejar" // Needed for cookie management
	"net/url"            // Needed for URL encoding form data
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"      // Requires: go get github.com/joho/godotenv
	"golang.org/x/net/publicsuffix" // Recommended for cookiejar
)

// Define constants for the Enphase cloud endpoints
const (
	// Login endpoint to get session ID and cookies (from Python example / PDF)
	// Use HTTPS for login as it's a sensitive endpoint.
	enlightenLoginURL = "https://enlighten.enphaseenergy.com/login/login.json?"
	// Token retrieval endpoint that returns JSON with token and expiry via GET (from PDF Page 5)
	// This URL requires an authenticated session (likely via cookies from login).
	tokenRetrievalBaseURL = "https://enlighten.enphaseenergy.com/entrez-auth-token?serial_num="
)

// Structs to match the expected JSON responses

// EnlightenLoginResponse matches the response from the login endpoint
type EnlightenLoginResponse struct {
	SessionID string `json:"session_id"`
	// The login endpoint might return other useful info, but session_id is key here.
	// We primarily rely on the cookies set by this response.
}

// EntrezTokenResponse matches the response from the token retrieval GET URL
type EntrezTokenResponse struct {
	GenerationTime int64  `json:"generation_time"`
	Token          string `json:"token"`
	ExpiresAt      int64  `json:"expires_at"` // Unix timestamp
}

// RefreshAndSaveOwnerToken logs in with owner credentials, gets an owner token, and saves it to the .env file.
func RefreshAndSaveOwnerToken() (string, int64, error) {
	log.Println("Attempting to refresh Enphase OWNER token using Login + Authenticated GET URL method...")
	// Call the internal refresh function with keys for owner credentials and token storage
	return refreshAndSaveTokenInternal("ENPHASE_USERNAME", "ENPHASE_PASSWORD", "ENVOY_TOKEN", "ENVOY_TOKEN_EXPIRY")
}

// RefreshAndSaveInstallerToken logs in with installer credentials, gets an installer token, and saves it to the .env file.
func RefreshAndSaveInstallerToken() (string, int64, error) {
	log.Println("Attempting to refresh Enphase INSTALLER token using Login + Authenticated GET URL method...")
	// Call the internal refresh function with keys for installer credentials and token storage
	return refreshAndSaveTokenInternal("ENPHASE_USERNAME_INSTALLER", "ENPHASE_PASSWORD_INSTALLER", "ENVOY_INSTALLER_TOKEN", "ENVOY_INSTALLER_TOKEN_EXPIRY")
}

// refreshAndSaveTokenInternal is a helper function to perform the token refresh logic for a given credential type.
// It takes environment variable keys for username, password, and the keys to use when saving the token/expiry to .env.
func refreshAndSaveTokenInternal(usernameEnvKey, passwordEnvKey, tokenEnvKey, expiryEnvKey string) (string, int64, error) {

	// Load environment variables from .env file to get credentials
	// We load again here to ensure we have the latest credentials if the .env file was edited manually
	err := godotenv.Load()
	if err != nil {
		log.Printf("Warning: Error loading .env file in token refresh: %v (This is okay if credentials are set as system env vars)", err)
	}

	// Read credentials and gateway serial from environment variables using the provided keys
	username := os.Getenv(usernameEnvKey)
	password := os.Getenv(passwordEnvKey)
	gatewaySerial := os.Getenv("ENPHASE_GATEWAY") // Gateway serial is the same for both token types

	if username == "" || password == "" || gatewaySerial == "" {
		return "", 0, fmt.Errorf("%s, %s, or ENPHASE_GATEWAY environment variables not set for token refresh", usernameEnvKey, passwordEnvKey)
	}

	// Create a cookie jar to manage cookies across requests
	// This is crucial because the token retrieval GET URL likely relies on session cookies from the login.
	jar, err := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	if err != nil {
		return "", 0, fmt.Errorf("error creating cookie jar: %w", err)
	}

	// Create an HTTP client that uses the cookie jar
	client := &http.Client{
		Timeout: 20 * time.Second, // Increased timeout slightly
		Jar:     jar,              // Attach the cookie jar to the client
	}

	// --- Step 1: Login to Enlighten to get a session ID and cookies ---
	log.Printf("Step 1: Logging in to Enlighten with %s credentials to get session cookies...", usernameEnvKey)

	// Prepare form data for login (using url.Values for application/x-www-form-urlencoded)
	loginData := url.Values{}
	loginData.Set("user[email]", username)
	loginData.Set("user[password]", password)

	// Make the POST request to the login endpoint
	respLogin, err := client.PostForm(enlightenLoginURL, loginData) // Use PostForm for url.Values
	if err != nil {
		return "", 0, fmt.Errorf("error making Enlighten login request to %s: %w", enlightenLoginURL, err)
	}
	defer respLogin.Body.Close()

	// Check for successful login status codes (200 OK or 302 Redirect)
	if respLogin.StatusCode != http.StatusOK && respLogin.StatusCode != http.StatusFound {
		bodyBytes, _ := io.ReadAll(respLogin.Body)
		return "", 0, fmt.Errorf("enlighten login returned non-OK status from %s: %d %s, Body: %s",
			enlightenLoginURL, respLogin.StatusCode, respLogin.Status, string(bodyBytes))
	}

	// At this point, if login was successful (200 OK or 302 Redirect), the client's cookie jar
	// should contain the session cookies set by the server.
	log.Printf("Step 1 completed with status: %d %s. Session cookies should be in the jar.",
		respLogin.StatusCode, respLogin.Status)

	// Optional: Read and parse the login response body for session_id, though relying on cookies is key
	// bodyLogin, err := io.ReadAll(respLogin.Body)
	// if err != nil {
	// 	log.Printf("Warning: Error reading Enlighten login response body: %v", err)
	// } else {
	// 	var loginResponse EnlightenLoginResponse
	// 	err = json.Unmarshal(bodyLogin, &loginResponse)
	// 	if err != nil {
	// 		log.Printf("Warning: Could not unmarshal Enlighten login response JSON: %v (Body: %s)", err, string(bodyLogin))
	// 	} else if loginResponse.SessionID != "" {
	// 		log.Printf("Login response contained session ID: %s", loginResponse.SessionID)
	// 	}
	// }

	// --- Step 2: Use the authenticated client (with cookies) to make the GET request for the token ---
	log.Println("Step 2: Requesting token from authenticated URL using session cookies...")

	tokenRetrievalURL := fmt.Sprintf("%s%s", tokenRetrievalBaseURL, gatewaySerial)
	log.Printf("Requesting token from URL: %s", tokenRetrievalURL)

	// Create the GET request object
	reqToken, err := http.NewRequest("GET", tokenRetrievalURL, nil)
	if err != nil {
		return "", 0, fmt.Errorf("error creating token retrieval GET request to %s: %w", tokenRetrievalURL, err)
	}
	// No need to manually add cookies here, the client.Do will use the jar attached to the client

	// Make the GET request using the client with the cookie jar
	respToken, err := client.Do(reqToken) // Use client.Do with the request object
	if err != nil {
		return "", 0, fmt.Errorf("error making token retrieval GET request to %s: %w", tokenRetrievalURL, err)
	}
	defer respToken.Body.Close()

	if respToken.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(respToken.Body)
		return "", 0, fmt.Errorf("token retrieval GET request to %s returned non-OK status: %d %s, Body: %s",
			tokenRetrievalURL, respToken.StatusCode, respToken.Status, string(bodyBytes))
	}

	// Read and parse the response body (expecting JSON with token and expiry)
	bodyToken, err := io.ReadAll(respToken.Body)
	if err != nil {
		return "", 0, fmt.Errorf("error reading token retrieval GET response body from %s: %w", tokenRetrievalURL, err)
	}

	var tokenResponse EntrezTokenResponse
	err = json.Unmarshal(bodyToken, &tokenResponse)
	if err != nil {
		return "", 0, fmt.Errorf("error unmarshalling token retrieval GET response JSON from %s: %w (Body: %s)", tokenRetrievalURL, err, string(bodyToken))
	}

	if tokenResponse.Token == "" || tokenResponse.ExpiresAt == 0 {
		return "", 0, fmt.Errorf("token retrieval GET response from %s did not contain token or expires_at (Body: %s)", tokenRetrievalURL, string(bodyToken))
	}

	log.Printf("Step 2 successful. Received new token expiring at %s (Unix: %d) using authenticated GET URL method.",
		time.Unix(tokenResponse.ExpiresAt, 0).UTC().Format(time.RFC3339), tokenResponse.ExpiresAt)

	// --- Step 3: Save the new token and expiry to the .env file ---
	log.Printf("Step 3: Saving new token and expiry to .env file using keys %s and %s...", tokenEnvKey, expiryEnvKey)

	// Read existing .env file content
	envMap, err := godotenv.Read()
	if err != nil {
		// If .env doesn't exist or can't be read, start with an empty map
		envMap = make(map[string]string)
		log.Printf("Warning: Could not read existing .env file, creating new map: %v", err)
	}

	// Update the token and expiry values in the map using the provided keys
	envMap[tokenEnvKey] = tokenResponse.Token
	envMap[expiryEnvKey] = strconv.FormatInt(tokenResponse.ExpiresAt, 10) // Save expiry as string

	// Write the updated map back to the .env file
	// godotenv.Write is a convenient way to write the map to a file, preserving format if possible
	err = godotenv.Write(envMap, ".env")
	if err != nil {
		return "", 0, fmt.Errorf("error writing to .env file: %w", err)
	}

	log.Println(".env file updated with new token and expiry.")

	// Return the new token and expiry
	return tokenResponse.Token, tokenResponse.ExpiresAt, nil
}

// This file defines functions within the 'tokenrefresher' package.
// It does not have a main function and is intended to be imported by other Go files.
