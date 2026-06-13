package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
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

func (s *Session) IsExpired() bool {
	return time.Now().After(s.ExpiresAt)
}

func (s *Session) IsFull() bool {
	return s.ActivePeers >= s.MaxPeers
}

func (s *Session) HasPassword() bool {
	return s.PasswordHash != ""
}

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

func (ss *SessionStore) Create(roomType, username, password string) *Session {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	code := generateUniqueCode(ss.sessions)
	s := &Session{
		Code:        code,
		RoomType:    roomType,
		MaxPeers:    4,
		ActivePeers: 0,
		CreatedAt:   time.Now(),
		ExpiresAt:   time.Now().Add(60 * time.Minute),
		CreatedBy:   username,
	}
	if password != "" {
		s.PasswordHash = hashPassword(password)
	}
	ss.sessions[code] = s
	log.Printf("[Session] Created %s (%s) by %s", code, roomType, username)
	return s
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
	ss.mu.Lock()
	defer ss.mu.Unlock()
	s, ok := ss.sessions[code]
	if !ok || s.IsExpired() || s.IsFull() {
		return false
	}
	s.ActivePeers++
	return true
}

func (ss *SessionStore) ReleaseSlot(code string) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	s, ok := ss.sessions[code]
	if !ok {
		return
	}
	if s.ActivePeers > 0 {
		s.ActivePeers--
	}
}

func (ss *SessionStore) CleanupExpired() int {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	count := 0
	for code, s := range ss.sessions {
		if s.IsExpired() && s.ActivePeers == 0 {
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

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string, errCode int) {
	writeJSON(w, status, map[string]interface{}{
		"error": message,
		"code":  errCode,
	})
}

// ============================================================
// API Types
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

// ============================================================
// Globals
// ============================================================

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	sfu   = NewSFU()
	store = NewSessionStore()
)

// ============================================================
// Main
// ============================================================

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("POST /api/sessions/create", handleCreateSession)
	mux.HandleFunc("POST /api/sessions/validate", handleValidateSession)
	mux.HandleFunc("POST /api/sessions/join/{code}", handleJoinSession)
	mux.HandleFunc("GET /ws/room/{code}/client/{clientID}", handleSignaling)

	handler := corsMiddleware(mux)

	go func() {
		for {
			time.Sleep(5 * time.Minute)
			count := store.CleanupExpired()
			if count > 0 {
				log.Printf("[Session] Cleaned up %d expired sessions", count)
			}
		}
	}()

	log.Println("[Server] VoiceVidChat Pion SFU listening on :8081")
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
// Health
// ============================================================

func handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]string{
		"status":  "healthy",
		"service": "VoiceVidChat Pion SFU",
	})
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

	session := store.Create(req.RoomType, req.Username, req.Password)

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

	writeJSON(w, 200, ValidateResponse{
		Code:     session.Code,
		RoomType: session.RoomType,
		MaxPeers: session.MaxPeers,
		CanJoin:  true,
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
// WS /ws/room/{code}/client/{clientID}
// ============================================================

func handleSignaling(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")
	clientID := r.PathValue("clientID")

	session := store.Get(code)
	if session == nil {
		http.Error(w, "Session not found or expired", 404)
		return
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[Server] Upgrade error: %v", err)
		return
	}
	defer ws.Close()

	displayName := r.URL.Query().Get("display_name")
	if displayName == "" {
		displayName = clientID
	}

	log.Printf("[Server] Client %s (%s) connected to room %s (%s)", clientID, displayName, code, session.RoomType)

	for {
		_, msgBytes, err := ws.ReadMessage()
		if err != nil {
			log.Printf("[Server] Read error from %s: %v", clientID, err)
			sfu.RemovePeer(code, clientID)
			store.ReleaseSlot(code)
			return
		}

		var msg map[string]interface{}
		if err := json.Unmarshal(msgBytes, &msg); err != nil {
			log.Printf("[Server] Invalid JSON from %s", clientID)
			continue
		}

		msgType, _ := msg["type"].(string)
		data, _ := msg["data"].(map[string]interface{})

		switch msgType {
		case "offer":
			if err := sfu.HandleOffer(code, session.RoomType, clientID, displayName, data, ws); err != nil {
				log.Printf("[Server] Offer handling failed for %s: %v", clientID, err)
				ws.WriteJSON(map[string]interface{}{
					"type":    "error",
					"message": err.Error(),
				})
				sfu.RemovePeer(code, clientID)
				store.ReleaseSlot(code)
				return
			}
		case "client_answer":
			sfu.HandleClientAnswer(code, clientID, data)
		case "pong":
			sfu.HandlePong(code, clientID)
		case "toggle_mute":
			muted, _ := data["muted"].(bool)
			sfu.HandleToggleMute(code, clientID, muted)
		default:
			log.Printf("[Server] Unknown message type '%s' from %s", msgType, clientID)
		}
	}
}
