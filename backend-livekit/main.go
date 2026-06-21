package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// ============================================================
// Globals
// ============================================================

var (
	livekit    *LiveKitService
	livekitCmd *exec.Cmd
	store      = NewSessionStore()
)

// ============================================================
// Session
// ============================================================

type Session struct {
	Code         string    `json:"code"`
	RoomType     string    `json:"room_type"`
	MaxPeers     int       `json:"max_peers"`
	ActivePeers  int       `json:"active_peers"`
	CreatedAt    time.Time `json:"created_at"`
	ExpiresAt    time.Time `json:"expires_at"`
	CreatedBy    string    `json:"created_by"`
	PasswordHash string    `json:"-"`
	mu           sync.Mutex
}

func (s *Session) IsExpired() bool  { return time.Now().After(s.ExpiresAt) }
func (s *Session) IsFull() bool     { return s.ActivePeers >= s.MaxPeers }
func (s *Session) HasPassword() bool { return s.PasswordHash != "" }
func (s *Session) CheckPassword(password string) bool {
	return s.PasswordHash == hashPassword(password)
}

// ============================================================
// SessionStore
// ============================================================

type SessionStore struct {
	sessions map[string]*Session
	mu       sync.RWMutex
}

func NewSessionStore() *SessionStore {
	return &SessionStore{sessions: make(map[string]*Session)}
}

func (ss *SessionStore) Create(roomType, username, password string) (*Session, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	code := generateUniqueCode(ss.sessions)

	// Create the LiveKit room first
	if livekit != nil {
		if err := livekit.CreateRoom(code); err != nil {
			return nil, err
		}
	}

	s := &Session{
		Code:       code,
		RoomType:   roomType,
		MaxPeers:   4,
		CreatedAt:  time.Now(),
		ExpiresAt:  time.Now().Add(60 * time.Minute),
		CreatedBy:  username,
	}
	if password != "" {
		s.PasswordHash = hashPassword(password)
	}
	ss.sessions[code] = s
	log.Printf("[Session] Created %s (%s) by %s", code, roomType, username)
	return s, nil
}

func (ss *SessionStore) Get(code string) *Session {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	s, ok := ss.sessions[code]
	if !ok || s.IsExpired() {
		return nil
	}
	return s
}

func (ss *SessionStore) ReserveSlot(code string) bool {
	// Slot management is handled by LiveKit's MaxParticipants.
	// Our session store no longer tracks active peer counts.
	return true
}

func (ss *SessionStore) ReleaseSlot(code string) {
	// Slot management is handled by LiveKit's MaxParticipants.
}

func (ss *SessionStore) CleanupExpired() int {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	count := 0
	for code, s := range ss.sessions {
		if s.IsExpired() {
			// Clean up the LiveKit room
			if livekit != nil {
				if err := livekit.DeleteRoom(code); err != nil {
					log.Printf("[Session] Failed to delete LiveKit room %s: %v", code, err)
				}
			}
			delete(ss.sessions, code)
			count++
		}
	}
	return count
}

// ============================================================
// Helpers
// ============================================================

const codeCharset = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

func generateCode() string {
	code := make([]byte, 6)
	for i := range code {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(codeCharset))))
		code[i] = codeCharset[n.Int64()]
	}
	return string(code)
}

func generateUniqueCode(existing map[string]*Session) string {
	for {
		code := generateCode()
		if _, exists := existing[code]; !exists {
			return code
		}
	}
}

func hashPassword(password string) string {
	h := sha256.Sum256([]byte(password))
	return hex.EncodeToString(h[:])
}

// ============================================================
// LiveKit Server Lifecycle
// ============================================================

func generateAPISecret() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func startLiveKitServer() error {
	// For dev/demo mode, use the standard devkey/secret
	// LiveKit --dev mode always uses these
	config := DefaultLiveKitConfig
	config.APIKey = "devkey"
	config.APISecret = "secret"

	// Try to find the LiveKit server binary
	binary := "livekit-server"
	if _, err := exec.LookPath(binary); err != nil {
		paths := []string{
			"./livekit-server",
			"./livekit-server.exe",
			"../livekit-server",
			"../livekit-server.exe",
		}
		found := false
		for _, p := range paths {
			if _, err := os.Stat(p); err == nil {
				binary = p
				found = true
				break
			}
		}
		if !found {
			log.Println("[LiveKit] Binary not found. Please place livekit-server.exe in backend-livekit/")
			log.Println("[LiveKit] Running in mock mode — room management will work but WebRTC won't.")
			return nil
		}
	}

	// Start in dev mode, bind to all interfaces so LAN/phone can reach it
	livekitCmd = exec.Command(binary, "--dev", "--bind", "0.0.0.0")
	livekitCmd.Stdout = os.Stdout
	livekitCmd.Stderr = os.Stderr

	if err := livekitCmd.Start(); err != nil {
		return fmt.Errorf("start livekit server: %w", err)
	}
	log.Printf("[LiveKit] Server started (PID %d) on :7880", livekitCmd.Process.Pid)

	// Detect LAN IP for multi-device testing
	if lanIP := getLANIP(); lanIP != "" {
		config.WSHost = "ws://" + lanIP + ":7880"
		log.Printf("[LiveKit] LAN IP detected: %s — phone can use ws://%s:7880", lanIP, lanIP)
	}
	livekit = NewLiveKitService(config)

	// Wait for the server to be ready
	time.Sleep(3 * time.Second)
	return nil
}

func getLANIP() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	var bestIP string
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ipnet, ok := addr.(*net.IPNet)
			if !ok || ipnet.IP.IsLoopback() {
				continue
			}
			if ipv4 := ipnet.IP.To4(); ipv4 != nil {
				// Prefer 192.168.x.x (most common WiFi LAN range)
				if ipv4[0] == 192 && ipv4[1] == 168 {
					return ipv4.String()
				}
				// Fallback: remember any private IP
				if bestIP == "" && isPrivateIPv4(ipv4) {
					bestIP = ipv4.String()
				}
			}
		}
	}
	return bestIP
}

func isPrivateIPv4(ip net.IP) bool {
	if ip[0] == 10 {
		return true
	}
	if ip[0] == 172 && ip[1] >= 16 && ip[1] <= 31 {
		return true
	}
	if ip[0] == 192 && ip[1] == 168 {
		return true
	}
	return false
}

func stopLiveKitServer() {
	if livekitCmd != nil && livekitCmd.Process != nil {
		log.Println("[LiveKit] Stopping server...")
		livekitCmd.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() {
			done <- livekitCmd.Wait()
		}()
		select {
		case <-done:
			log.Println("[LiveKit] Server stopped")
		case <-time.After(5 * time.Second):
			log.Println("[LiveKit] Force killing server...")
			livekitCmd.Process.Kill()
		}
	}
}

// ============================================================
// Main
// ============================================================

func main() {
	// Start LiveKit server
	if err := startLiveKitServer(); err != nil {
		log.Fatalf("[Server] Failed to start LiveKit: %v", err)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("POST /api/sessions/create", handleCreateSession)
	mux.HandleFunc("POST /api/sessions/validate", handleValidateSession)
	mux.HandleFunc("POST /api/sessions/join/{code}", handleJoinSession)
	mux.HandleFunc("POST /api/livekit/token", handleGetToken)

	// Proxy LiveKit WebSocket through our server so HTTPS (ngrok) works
	// even for WebRTC connections. This avoids mixed-content blocks on phone.
	mux.HandleFunc("GET /rtc/", handleLiveKitProxy)
	mux.Handle("GET /rtc", http.RedirectHandler("/rtc/", 301))

	// Serve frontend static files
	fileServer := http.FileServer(http.Dir("../frontend"))
	mux.Handle("/", fileServer)

	handler := corsMiddleware(mux)

	// Session expiry cleanup goroutine
	go func() {
		for {
			time.Sleep(5 * time.Minute)
			count := store.CleanupExpired()
			if count > 0 {
				log.Printf("[Session] Cleaned up %d expired sessions", count)
			}
		}
	}()

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("[Server] Shutting down...")
		stopLiveKitServer()
		os.Exit(0)
	}()

	log.Println("[Server] VoiceVidChat LiveKit SFU listening on :8081")
	if err := http.ListenAndServe(":8081", handler); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

// ============================================================
// CORS
// ============================================================

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ============================================================
// JSON helpers
// ============================================================

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string, code int) {
	writeJSON(w, status, map[string]interface{}{
		"error": message,
		"code":  code,
	})
}

// ============================================================
// Health
// ============================================================

func handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]string{
		"status":  "healthy",
		"service": "VoiceVidChat LiveKit SFU",
	})
}

// ============================================================
// API types
// ============================================================

type CreateRequest struct {
	RoomType string `json:"room_type"`
	Username string `json:"username"`
	Password string `json:"password,omitempty"`
}

type CreateResponse struct {
	Code     string `json:"code"`
	RoomType string `json:"room_type"`
	MaxPeers int    `json:"max_peers"`
}

type ValidateRequest struct {
	Code     string `json:"code"`
	Password string `json:"password,omitempty"`
}

type ValidateResponse struct {
	Code     string `json:"code"`
	RoomType string `json:"room_type"`
	MaxPeers int    `json:"max_peers"`
	CanJoin  bool   `json:"can_join"`
	LiveKitURL string `json:"livekit_url,omitempty"`
}

type JoinRequest struct {
	Username string `json:"username"`
	Password string `json:"password,omitempty"`
}

type JoinResponse struct {
	Joined   bool   `json:"joined"`
	Code     string `json:"code"`
	RoomType string `json:"room_type"`
}

type TokenRequest struct {
	Code         string `json:"code"`
	Identity     string `json:"identity"`
	DisplayName  string `json:"display_name"`
}

type TokenResponse struct {
	Token string `json:"token"`
	URL   string `json:"url"`
}

// ============================================================
// POST /api/sessions/create
// ============================================================

func handleCreateSession(w http.ResponseWriter, r *http.Request) {
	var req CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "Invalid request body", 4000)
		return
	}
	if req.RoomType != "voice" && req.RoomType != "video" {
		writeError(w, 400, "room_type must be 'voice' or 'video'", 4001)
		return
	}
	if req.Username == "" {
		writeError(w, 400, "username is required", 4002)
		return
	}

	session, err := store.Create(req.RoomType, req.Username, req.Password)
	if err != nil {
		writeError(w, 500, "Failed to create session: "+err.Error(), 5000)
		return
	}

	writeJSON(w, 201, CreateResponse{
		Code:     session.Code,
		RoomType: session.RoomType,
		MaxPeers: session.MaxPeers,
	})
}

// ============================================================
// POST /api/sessions/validate
// ============================================================

func handleValidateSession(w http.ResponseWriter, r *http.Request) {
	var req ValidateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "Invalid request body", 4000)
		return
	}
	if req.Code == "" {
		writeError(w, 400, "code is required", 4003)
		return
	}

	session := store.Get(req.Code)
	if session == nil {
		writeError(w, 404, "Session not found or expired", 4040)
		return
	}
	if session.IsFull() {
		writeError(w, 409, "Session is full", 4090)
		return
	}
	if session.HasPassword() && !session.CheckPassword(req.Password) {
		writeError(w, 403, "Invalid password", 4030)
		return
	}

	livekitURL := DefaultLiveKitConfig.WSHost
	if livekit != nil {
		livekitURL = livekit.config.WSHost
	}

	writeJSON(w, 200, ValidateResponse{
		Code:       session.Code,
		RoomType:   session.RoomType,
		MaxPeers:   session.MaxPeers,
		CanJoin:    true,
		LiveKitURL: livekitURL,
	})
}

// ============================================================
// POST /api/sessions/join/{code}
// ============================================================

func handleJoinSession(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")

	var req JoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "Invalid request body", 4000)
		return
	}
	if req.Username == "" {
		writeError(w, 400, "username is required", 4002)
		return
	}

	session := store.Get(code)
	if session == nil {
		writeError(w, 404, "Session not found or expired", 4040)
		return
	}
	if session.HasPassword() && !session.CheckPassword(req.Password) {
		writeError(w, 403, "Invalid password", 4030)
		return
	}
	if !store.ReserveSlot(code) {
		writeError(w, 409, "Session is full", 4090)
		return
	}

	writeJSON(w, 200, JoinResponse{
		Joined:   true,
		Code:     session.Code,
		RoomType: session.RoomType,
	})
}

// ============================================================
// POST /api/livekit/token
// ============================================================

func handleGetToken(w http.ResponseWriter, r *http.Request) {
	var req TokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "Invalid request body", 4000)
		return
	}
	if req.Code == "" || req.Identity == "" {
		writeError(w, 400, "code and identity are required", 4003)
		return
	}

	session := store.Get(req.Code)
	if session == nil {
		writeError(w, 404, "Session not found or expired", 4040)
		return
	}

	if livekit == nil {
		writeError(w, 503, "LiveKit server not available", 5030)
		return
	}

	token, err := livekit.GenerateToken(req.Code, req.Identity, req.DisplayName)
	if err != nil {
		writeError(w, 500, "Failed to generate token: "+err.Error(), 5001)
		return
	}

	writeJSON(w, 200, TokenResponse{
		Token: token,
		URL:   livekitProxyURL(r),
	})
}

// livekitProxyURL returns the appropriate LiveKit URL for the client.
// If the request came through ngrok/HTTPS, return the proxy URL on our server.
// If local, return the direct LiveKit WebSocket URL.
func livekitProxyURL(r *http.Request) string {
	// If the request came over HTTPS, the client can't use ws://.
	// Return a URL proxied through our server instead.
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme := "wss"
		host := r.Host
		return scheme + "://" + host + "/rtc/"
	}
	// For local requests, use localhost directly (faster, no loopback issues)
	// For LAN requests, use the detected LAN IP
	clientIP := r.RemoteAddr
	if isLocalRequest(clientIP) {
		return "ws://localhost:7880"
	}
	return livekit.config.WSHost
}

func isLocalRequest(addr string) bool {
	// RemoteAddr is "ip:port", extract IP
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback() || ip.Equal(net.ParseIP("::1"))
}

// ============================================================
// LiveKit Reverse Proxy
// ============================================================

func handleLiveKitProxy(w http.ResponseWriter, r *http.Request) {
	// Rewrite the URL to point to the LiveKit server
	target := &url.URL{
		Scheme: "http",
		Host:   "localhost:7880",
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ServeHTTP(w, r)
}
