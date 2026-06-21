package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ============================================================
// LiveKit Configuration
// ============================================================

type LiveKitConfig struct {
	Host      string // LiveKit server HTTP URL, e.g. "http://localhost:7880"
	WSHost    string // LiveKit server WebSocket URL, e.g. "ws://localhost:7880"
	APIKey    string
	APISecret string
}

var DefaultLiveKitConfig = LiveKitConfig{
	Host:      "http://localhost:7880",
	WSHost:    "ws://localhost:7880",
	APIKey:    "VOICEVIDCHAT_API_KEY",
	APISecret: "",
}

// ============================================================
// LiveKitService
// ============================================================

type LiveKitService struct {
	config LiveKitConfig
	client *http.Client
}

func NewLiveKitService(cfg LiveKitConfig) *LiveKitService {
	return &LiveKitService{
		config: cfg,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// twirpRequest makes an authenticated POST request to a LiveKit Twirp endpoint.
// Uses a server-admin JWT (Bearer token) for auth.
func (l *LiveKitService) twirpRequest(endpoint string, reqBody, respBody interface{}) error {
	var bodyBytes []byte
	if reqBody != nil {
		var err error
		bodyBytes, err = json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
	}

	// Generate an admin JWT for server-to-server API calls.
	token, err := l.generateAdminToken()
	if err != nil {
		return fmt.Errorf("admin token: %w", err)
	}

	url := l.config.Host + "/twirp/livekit.RoomService/" + endpoint
	httpReq, err := http.NewRequest("POST", url, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := l.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("livekit API error (%d): %s", resp.StatusCode, string(respBytes))
	}

	if respBody != nil {
		if err := json.Unmarshal(respBytes, respBody); err != nil {
			return fmt.Errorf("unmarshal response: %w", err)
		}
	}
	return nil
}

// generateAdminToken creates a server admin JWT with RoomAdmin + RoomCreate grants.
func (l *LiveKitService) generateAdminToken() (string, error) {
	now := time.Now()
	claims := livekitClaims{
		Name: "server-admin",
		Video: videoGrant{
			RoomJoin:   true,
			CanPublish: true,
			CanSubscribe: true,
		},
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    l.config.APIKey,
			Subject:   "server-admin",
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(5 * time.Minute)),
		},
	}
	// Add admin grants
	claims.Video.RoomCreate = true
	claims.Video.RoomList = true
	claims.Video.RoomAdmin = true

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(l.config.APISecret))
}

// ============================================================
// Room Management
// ============================================================

type createRoomRequest struct {
	Name            string `json:"name"`
	EmptyTimeout    int    `json:"empty_timeout"`
	MaxParticipants int    `json:"max_participants"`
}

type roomResponse struct {
	Name string `json:"name"`
	Sid  string `json:"sid"`
}

func (l *LiveKitService) CreateRoom(roomID string) error {
	req := createRoomRequest{
		Name:            roomID,
		EmptyTimeout:    300,
		MaxParticipants: 5,
	}
	var resp roomResponse
	if err := l.twirpRequest("CreateRoom", req, &resp); err != nil {
		return err
	}
	log.Printf("[LiveKit] Created room %s (sid=%s)", resp.Name, resp.Sid)
	return nil
}

type deleteRoomRequest struct {
	Room string `json:"room"`
}

func (l *LiveKitService) DeleteRoom(roomID string) error {
	req := deleteRoomRequest{Room: roomID}
	if err := l.twirpRequest("DeleteRoom", req, nil); err != nil {
		return err
	}
	log.Printf("[LiveKit] Deleted room %s", roomID)
	return nil
}

type listParticipantsRequest struct {
	Room string `json:"room"`
}

type listParticipantsResponse struct {
	Participants []map[string]interface{} `json:"participants"`
}

func (l *LiveKitService) GetParticipantCount(roomID string) (int, error) {
	req := listParticipantsRequest{Room: roomID}
	var resp listParticipantsResponse
	if err := l.twirpRequest("ListParticipants", req, &resp); err != nil {
		return 0, err
	}
	return len(resp.Participants), nil
}

// ============================================================
// Token Generation (JWT)
// ============================================================

type videoGrant struct {
	Room         string `json:"room,omitempty"`
	RoomJoin     bool   `json:"roomJoin,omitempty"`
	RoomCreate   bool   `json:"roomCreate,omitempty"`
	RoomList     bool   `json:"roomList,omitempty"`
	RoomAdmin    bool   `json:"roomAdmin,omitempty"`
	CanPublish   bool   `json:"canPublish,omitempty"`
	CanSubscribe bool   `json:"canSubscribe,omitempty"`
}

type livekitClaims struct {
	Name  string     `json:"name"`
	Video videoGrant `json:"video"`
	jwt.RegisteredClaims
}

func (l *LiveKitService) GenerateToken(roomID, identity, displayName string) (string, error) {
	now := time.Now()
	claims := livekitClaims{
		Name: displayName,
		Video: videoGrant{
			Room:         roomID,
			RoomJoin:     true,
			CanPublish:   true,
			CanSubscribe: true,
		},
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    l.config.APIKey,
			Subject:   identity,
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(2 * time.Hour)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, err := token.SignedString([]byte(l.config.APISecret))
	if err != nil {
		return "", fmt.Errorf("sign token: %w", err)
	}
	return tokenStr, nil
}

// ============================================================
// Errors
// ============================================================

var ErrorRoomFull = &RoomFullError{}

type RoomFullError struct{}

func (e *RoomFullError) Error() string {
	return "room is full (max 5 peers)"
}
