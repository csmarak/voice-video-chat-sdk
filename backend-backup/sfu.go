package main

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
)

// api is the singleton WebRTC API with Pion's built-in interceptor pipeline:
// NACK responder/receiver, sender/receiver reports, and TWCC.
var api *webrtc.API

func init() {
	var err error
	api, err = createWebRTCAPI()
	if err != nil {
		log.Fatalf("[SFU] Failed to create WebRTC API: %v", err)
	}
}

func createWebRTCAPI() (*webrtc.API, error) {
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}

	ir := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(m, ir); err != nil {
		return nil, err
	}

	return webrtc.NewAPI(
		webrtc.WithMediaEngine(m),
		webrtc.WithInterceptorRegistry(ir),
	), nil
}

const (
	MaxPeersPerRoom    = 4
	HeartbeatInterval  = 10 * time.Second
	HeartbeatTimeout   = 30 * time.Second
)

// ============================================================
// Types
// ============================================================

type Room struct {
	ID       string
	RoomType string
	Peers    map[string]*Peer
	mu       sync.RWMutex
}

type Peer struct {
	ID          string
	DisplayName string
	PC          *webrtc.PeerConnection
	WS          *websocket.Conn
	wsMu        sync.Mutex
	Room        *Room
	remoteAudio *webrtc.TrackRemote
	remoteVideo *webrtc.TrackRemote
	IsMuted     atomic.Bool
	lastPong    atomic.Int64
	alive       atomic.Bool
	mu          sync.Mutex
	disconnectTimer *time.Timer
	negTimer    *time.Timer
}

type SFU struct {
	Rooms map[string]*Room
	mu    sync.RWMutex
}

var peerConfig = webrtc.Configuration{
	ICEServers: []webrtc.ICEServer{
		{URLs: []string{"stun:stun.l.google.com:19302"}},
	},
}

// ============================================================
// SFU constructor
// ============================================================

func NewSFU() *SFU {
	return &SFU{Rooms: make(map[string]*Room)}
}

// ============================================================
// Room management
// ============================================================

func (s *SFU) GetOrCreateRoom(roomID, roomType string) *Room {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.Rooms[roomID]; ok {
		return r
	}
	r := &Room{ID: roomID, RoomType: roomType, Peers: make(map[string]*Peer)}
	s.Rooms[roomID] = r
	log.Printf("[SFU] Created room %s (%s)", roomID, roomType)
	return r
}

func (s *SFU) RoomCount(roomID string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	room, ok := s.Rooms[roomID]
	if !ok {
		return 0
	}
	room.mu.RLock()
	defer room.mu.RUnlock()
	return len(room.Peers)
}

// ============================================================
// Helpers
// ============================================================

func (p *Peer) SendJSON(v interface{}) error {
	p.wsMu.Lock()
	defer p.wsMu.Unlock()
	return p.WS.WriteJSON(v)
}

// ============================================================
// Heartbeat
// ============================================================

func (s *SFU) HandlePong(roomID, clientID string) {
	room, ok := s.Rooms[roomID]
	if !ok {
		return
	}
	room.mu.RLock()
	peer, ok := room.Peers[clientID]
	room.mu.RUnlock()
	if ok {
		peer.lastPong.Store(time.Now().UnixMilli())
	}
}

func (s *SFU) startHeartbeat(roomID string, peer *Peer) {
	peer.alive.Store(true)
	peer.lastPong.Store(time.Now().UnixMilli())

	go func() {
		for peer.alive.Load() {
			time.Sleep(HeartbeatInterval)
			if !peer.alive.Load() {
				return
			}
			err := peer.SendJSON(map[string]interface{}{"type": "ping"})
			if err != nil {
				log.Printf("[SFU] Heartbeat send failed for %s", peer.ID)
				s.RemovePeerIfMatching(roomID, peer.ID, peer)
				return
			}
		}
	}()

	go func() {
		for peer.alive.Load() {
			time.Sleep(5 * time.Second)
			if !peer.alive.Load() {
				return
			}
			if time.Now().UnixMilli()-peer.lastPong.Load() > int64(HeartbeatTimeout/time.Millisecond) {
				log.Printf("[SFU] Heartbeat timeout for %s", peer.ID)
				s.RemovePeerIfMatching(roomID, peer.ID, peer)
				return
			}
		}
	}()
}

// RemovePeerIfMatching protects active sessions from being murdered by ghost timers.
// It only removes the peer if the instance currently in the room's Peers map matches
// the exact memory address of the tracking goroutine's peer reference.
func (s *SFU) RemovePeerIfMatching(roomID, clientID string, checkingPeer *Peer) {
	room, ok := s.Rooms[roomID]
	if !ok {
		return
	}

	room.mu.Lock()
	currentActivePeer, exists := room.Peers[clientID]

	// CRITICAL GUARD: Only delete if the peer in the map is the EXACT same
	// object instance as the one this goroutine was tracking.
	if exists && currentActivePeer == checkingPeer {
		checkingPeer.alive.Store(false)

		checkingPeer.mu.Lock()
		if checkingPeer.disconnectTimer != nil {
			checkingPeer.disconnectTimer.Stop()
			checkingPeer.disconnectTimer = nil
		}
		if checkingPeer.negTimer != nil {
			checkingPeer.negTimer.Stop()
			checkingPeer.negTimer = nil
		}
		checkingPeer.mu.Unlock()

		delete(room.Peers, clientID)
		peerCount := len(room.Peers)
		room.mu.Unlock()

		log.Printf("[SFU-Guard] Safe-evicted timed out session for %s (%d left)", clientID, peerCount)

		if checkingPeer.PC != nil {
			_ = checkingPeer.PC.Close()
		}
		checkingPeer.wsMu.Lock()
		_ = checkingPeer.WS.Close()
		checkingPeer.wsMu.Unlock()

		s.broadcastToRoomLocked(room, clientID, map[string]interface{}{
			"type": "user_left",
			"data": map[string]interface{}{
				"client_id":    clientID,
				"display_name": checkingPeer.DisplayName,
			},
		})
		s.broadcastRoomState(room)

		if peerCount == 0 {
			s.mu.Lock()
			delete(s.Rooms, roomID)
			s.mu.Unlock()
			log.Printf("[SFU] Room %s destroyed (empty)", roomID)
		}
	} else {
		// The ghost loop woke up, but a new valid connection took the slot!
		room.mu.Unlock()
		log.Printf("[SFU-Guard] Aborted ghost eviction. Valid active session detected for %s", clientID)
	}
}

// ============================================================
// Peer removal
// ============================================================

func (s *SFU) RemovePeer(roomID, clientID string) {
	room, ok := s.Rooms[roomID]
	if !ok {
		return
	}

	room.mu.Lock()
	peer, ok := room.Peers[clientID]
	if !ok {
		room.mu.Unlock()
		return
	}
	peer.alive.Store(false)

	peer.mu.Lock()
	if peer.disconnectTimer != nil {
		peer.disconnectTimer.Stop()
		peer.disconnectTimer = nil
	}
	if peer.negTimer != nil {
		peer.negTimer.Stop()
		peer.negTimer = nil
	}
	peer.mu.Unlock()

	delete(room.Peers, clientID)
	peerCount := len(room.Peers)
	room.mu.Unlock()

	log.Printf("[SFU] Peer %s removed from room %s (%d remaining)", clientID, roomID, peerCount)

	if peer.PC != nil {
		_ = peer.PC.Close()
	}

	s.broadcastToRoomLocked(room, clientID, map[string]interface{}{
		"type": "user_left",
		"data": map[string]interface{}{
			"client_id":    clientID,
			"display_name": peer.DisplayName,
		},
	})

	s.broadcastRoomState(room)

	if peerCount == 0 {
		s.mu.Lock()
		delete(s.Rooms, roomID)
		s.mu.Unlock()
		log.Printf("[SFU] Room %s destroyed (empty)", roomID)
	}
}

// RemovePeerForConnection only removes the peer if its WebSocket matches the
// one passed in. This prevents a stale handleSignaling goroutine from nuking
// a new peer that reconnected under the same clientID.
func (s *SFU) RemovePeerForConnection(roomID, clientID string, ws *websocket.Conn) {
	room, ok := s.Rooms[roomID]
	if !ok {
		return
	}

	room.mu.Lock()
	peer, ok := room.Peers[clientID]
	if !ok {
		room.mu.Unlock()
		return
	}

	// The peer's WS doesn't match — a new connection with the same
	// clientID has already taken over. Leave it alone.
	if peer.WS != ws {
		room.mu.Unlock()
		log.Printf("[SFU] Stale handleSignaling for %s aborted (WS mismatch)", clientID)
		return
	}
	room.mu.Unlock()

	// WS matches — proceed with normal removal
	s.RemovePeer(roomID, clientID)
}

// ============================================================
// Broadcast helpers
// ============================================================

func (s *SFU) broadcastToRoomLocked(room *Room, excludeID string, msg map[string]interface{}) {
	for _, p := range room.Peers {
		if p.ID == excludeID {
			continue
		}
		if err := p.SendJSON(msg); err != nil {
			log.Printf("[SFU] Broadcast send failed to %s: %v", p.ID, err)
		}
	}
}

func (s *SFU) broadcastRoomState(room *Room) {
	users := make([]map[string]interface{}, 0)
	for _, p := range room.Peers {
		users = append(users, map[string]interface{}{
			"client_id":    p.ID,
			"display_name": p.DisplayName,
			"is_muted":     p.IsMuted.Load(),
		})
	}
	msg := map[string]interface{}{
		"type": "room_state",
		"data": map[string]interface{}{
			"room_type": room.RoomType,
			"users":     users,
		},
	}
	for _, p := range room.Peers {
		if err := p.SendJSON(msg); err != nil {
			log.Printf("[SFU] Room state send failed to %s: %v", p.ID, err)
		}
	}
}

// ============================================================
// Track relay
// ============================================================

func pumpRemoteToLocal(remote *webrtc.TrackRemote, local *webrtc.TrackLocalStaticRTP, sender *webrtc.RTPSender) {
	go func() {
		buf := make([]byte, 1500)
		for {
			i, _, readErr := remote.Read(buf)
			if readErr != nil {
				return
			}
			if _, writeErr := local.Write(buf[:i]); writeErr != nil {
				return
			}
		}
	}()
	go func() {
		rtcpBuf := make([]byte, 1500)
		for {
			if _, _, rtcpErr := sender.Read(rtcpBuf); rtcpErr != nil {
				return
			}
		}
	}()
}

func (s *SFU) forwardTrackToPeer(peer *Peer, track *webrtc.TrackRemote, sourceClientID string) error {
	newTrack, err := webrtc.NewTrackLocalStaticRTP(
		track.Codec().RTPCodecCapability,
		track.ID(),
		sourceClientID,
	)
	if err != nil {
		return err
	}

	sender, err := peer.PC.AddTrack(newTrack)
	if err != nil {
		return err
	}

	pumpRemoteToLocal(track, newTrack, sender)
	return nil
}

func (p *Peer) SignalNegotiation() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Batching debounce — reset the timer on every call so multiple
	// adjacent track additions coalesce into a single SDP offer.
	if p.negTimer != nil {
		p.negTimer.Stop()
	}

	p.negTimer = time.AfterFunc(100*time.Millisecond, func() {
		p.mu.Lock()
		defer p.mu.Unlock()

		if p.PC == nil {
			return
		}
		if p.PC.SignalingState() != webrtc.SignalingStateStable {
			// Ongoing negotiation — retry after it settles
			p.negTimer = time.AfterFunc(100*time.Millisecond, func() {
				p.SignalNegotiation()
			})
			return
		}

		offer, err := p.PC.CreateOffer(nil)
		if err != nil {
			log.Printf("[SFU] CreateOffer error for %s: %v", p.ID, err)
			return
		}
		if err = p.PC.SetLocalDescription(offer); err != nil {
			log.Printf("[SFU] SetLocalDescription error for %s: %v", p.ID, err)
			return
		}
		// Trickle ICE: send the offer immediately without waiting for
		// full candidate gathering. ICE candidates will be trickled
		// by the OnICECandidate handler registered on this PC.
		if err := p.SendJSON(map[string]interface{}{
			"type": "server_offer",
			"data": map[string]interface{}{
				"sdp":  p.PC.LocalDescription().SDP,
				"type": "offer",
			},
		}); err != nil {
			log.Printf("[SFU] Renegotiation send failed for %s: %v", p.ID, err)
		}
	})
}

func (s *SFU) setupOnNegotiationNeeded(peer *Peer) {
	peer.PC.OnNegotiationNeeded(func() {
		peer.SignalNegotiation()
	})
}

// ============================================================
// HandleClientAnswer (renegotiation response)
// ============================================================

func (s *SFU) HandleClientAnswer(roomID, clientID string, answerDict map[string]interface{}) {
	room, ok := s.Rooms[roomID]
	if !ok {
		return
	}
	room.mu.RLock()
	peer, ok := room.Peers[clientID]
	room.mu.RUnlock()
	if !ok {
		return
	}

	sdp, _ := answerDict["sdp"].(string)
	answer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  sdp,
	}
	if err := peer.PC.SetRemoteDescription(answer); err != nil {
		log.Printf("[SFU] Failed to set remote answer for %s: %v", clientID, err)
	}
}

// ============================================================
// HandleIceCandidate (Trickle ICE)
// ============================================================

func (s *SFU) HandleIceCandidate(roomID, clientID string, candidateDict map[string]interface{}) {
	room, ok := s.Rooms[roomID]
	if !ok {
		return
	}
	room.mu.RLock()
	peer, ok := room.Peers[clientID]
	room.mu.RUnlock()
	if !ok || peer.PC == nil {
		return
	}

	candidateStr, _ := candidateDict["candidate"].(string)
	if candidateStr == "" {
		return
	}
	sdpMid, _ := candidateDict["sdpMid"].(string)
	mlIndexFloat, _ := candidateDict["sdpMLineIndex"].(float64)
	mlIndex := uint16(mlIndexFloat)

	init := webrtc.ICECandidateInit{
		Candidate:     candidateStr,
		SDPMid:        &sdpMid,
		SDPMLineIndex: &mlIndex,
	}
	if err := peer.PC.AddICECandidate(init); err != nil {
		log.Printf("[SFU] Failed to add ICE candidate for %s: %v", clientID, err)
	}
}

// ============================================================
// HandleToggleMute
// ============================================================

func (s *SFU) HandleToggleMute(roomID, clientID string, muted bool) {
	room, ok := s.Rooms[roomID]
	if !ok {
		return
	}
	room.mu.RLock()
	peer, ok := room.Peers[clientID]
	room.mu.RUnlock()
	if !ok {
		return
	}
	peer.IsMuted.Store(muted)

	msg := map[string]interface{}{
		"type": "user_muted",
		"data": map[string]interface{}{
			"client_id": clientID,
			"muted":     muted,
		},
	}
	s.broadcastToRoomLocked(room, clientID, msg)
}

// ============================================================
// HandleOffer — main SFU join handler
// ============================================================

func (s *SFU) HandleOffer(roomID, roomType, clientID, displayName string, offerDict map[string]interface{}, ws *websocket.Conn) error {
	room := s.GetOrCreateRoom(roomID, roomType)

	s.mu.RLock()
	currentRoom := s.Rooms[roomID]
	s.mu.RUnlock()

	currentRoom.mu.RLock()
	peerCount := len(currentRoom.Peers)
	currentRoom.mu.RUnlock()

	if peerCount >= MaxPeersPerRoom {
		return ErrorRoomFull
	}

	pc, err := api.NewPeerConnection(peerConfig)
	if err != nil {
		return err
	}

	peer := &Peer{ID: clientID, DisplayName: displayName, PC: pc, WS: ws, Room: room}

	s.setupOnNegotiationNeeded(peer)

	// Trickle ICE: relay candidates to the client as they're gathered
	// instead of waiting for GatheringCompletePromise.
	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return // gathering complete
		}
		init := candidate.ToJSON()
		var mlIndex int
		if init.SDPMLineIndex != nil {
			mlIndex = int(*init.SDPMLineIndex)
		}
		if err := peer.SendJSON(map[string]interface{}{
			"type": "ice_candidate",
			"data": map[string]interface{}{
				"candidate":     init.Candidate,
				"sdpMid":        init.SDPMid,
				"sdpMLineIndex": mlIndex,
			},
		}); err != nil {
			log.Printf("[SFU] Failed to send ICE candidate to %s: %v", clientID, err)
		}
	})

	room.mu.RLock()
	for _, p := range room.Peers {
		if p.PC != nil {
			s.setupOnNegotiationNeeded(p)
		}
	}
	room.mu.RUnlock()

	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		log.Printf("[SFU] Track arrived: kind=%s from %s", track.Kind(), clientID)

		if room.RoomType == "voice" && track.Kind() == webrtc.RTPCodecTypeVideo {
			log.Printf("[SFU] Ignoring video track in voice room from %s", clientID)
			return
		}

		switch track.Kind() {
		case webrtc.RTPCodecTypeAudio:
			peer.remoteAudio = track
		case webrtc.RTPCodecTypeVideo:
			peer.remoteVideo = track
		}

		room.mu.RLock()
		defer room.mu.RUnlock()

		for _, p := range room.Peers {
			if p.ID == clientID {
				continue
			}
			if p.PC == nil || p.PC.ConnectionState() == webrtc.PeerConnectionStateClosed {
				continue
			}
			if err := s.forwardTrackToPeer(p, track, clientID); err != nil {
				log.Printf("[SFU] Failed to forward track to %s: %v", p.ID, err)
			} else {
				log.Printf("[SFU] Forwarded track from %s to %s", clientID, p.ID)
			}
		}
	})

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Printf("[SFU] Peer %s connection state: %s", clientID, state)
		switch state {
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			s.RemovePeerIfMatching(roomID, clientID, peer)
		case webrtc.PeerConnectionStateDisconnected:
			// Grace period — mobile networks frequently glitch for 3-5s
			peer.mu.Lock()
			if peer.disconnectTimer != nil {
				peer.disconnectTimer.Stop()
			}
			peer.disconnectTimer = time.AfterFunc(10*time.Second, func() {
				log.Printf("[SFU] Grace timer expired for %s, removing", clientID)
				s.RemovePeerIfMatching(roomID, clientID, peer)
			})
			peer.mu.Unlock()
		case webrtc.PeerConnectionStateConnected:
			// Reconnected — cancel the grace timer
			peer.mu.Lock()
			if peer.disconnectTimer != nil {
				peer.disconnectTimer.Stop()
				peer.disconnectTimer = nil
				log.Printf("[SFU] Peer %s reconnected, grace timer cancelled", clientID)
			}
			peer.mu.Unlock()
		}
	})

	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Printf("[SFU] Peer %s ICE state: %s", clientID, state)
		if state == webrtc.ICEConnectionStateFailed ||
			state == webrtc.ICEConnectionStateClosed {
			s.RemovePeerIfMatching(roomID, clientID, peer)
		}
	})

	room.mu.Lock()
	// If a peer with the same clientID already exists (reconnect race),
	// evict it cleanly: stop timers, remove from map, then close its PC
	// and WS outside the lock to avoid deadlock with the old goroutine.
	if oldPeer, exists := room.Peers[clientID]; exists {
		oldPeer.alive.Store(false)
		if oldPeer.disconnectTimer != nil {
			oldPeer.disconnectTimer.Stop()
		}
		if oldPeer.negTimer != nil {
			oldPeer.negTimer.Stop()
		}
		delete(room.Peers, clientID)
		room.mu.Unlock()

		log.Printf("[SFU] Evicting stale peer %s in room %s", clientID, roomID)
		if oldPeer.PC != nil {
			_ = oldPeer.PC.Close()
		}
		oldPeer.wsMu.Lock()
		_ = oldPeer.WS.Close()
		oldPeer.wsMu.Unlock()

		room.mu.Lock()
	}
	room.Peers[clientID] = peer
	newPeerCount := len(room.Peers)
	room.mu.Unlock()

	log.Printf("[SFU] Peer %s joined room %s (%d/%d)", clientID, roomID, newPeerCount, MaxPeersPerRoom)

	sdp, _ := offerDict["sdp"].(string)
	offer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  sdp,
	}
	if err := pc.SetRemoteDescription(offer); err != nil {
		return err
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return err
	}
	if err = pc.SetLocalDescription(answer); err != nil {
		return err
	}

	// Trickle ICE: send the answer immediately without waiting for
	// full candidate gathering. ICE candidates will be trickled
	// by the OnICECandidate handler registered above.

	// Send room_state to the new peer (includes the new peer themselves)
	users := make([]map[string]interface{}, 0)
	room.mu.RLock()
	for _, p := range room.Peers {
		users = append(users, map[string]interface{}{
			"client_id":    p.ID,
			"display_name": p.DisplayName,
			"is_muted":     p.IsMuted.Load(),
		})
	}
	room.mu.RUnlock()

	if err := peer.SendJSON(map[string]interface{}{
		"type": "answer",
		"data": map[string]interface{}{
			"sdp":  pc.LocalDescription().SDP,
			"type": "answer",
		},
	}); err != nil {
		return err
	}

	if err := peer.SendJSON(map[string]interface{}{
		"type": "room_state",
		"data": map[string]interface{}{
			"room_type": room.RoomType,
			"users":     users,
		},
	}); err != nil {
		log.Printf("[SFU] Failed to send room_state to %s: %v", clientID, err)
	}

	// Broadcast user_joined to existing peers
	room.mu.RLock()
	s.broadcastToRoomLocked(room, clientID, map[string]interface{}{
		"type": "user_joined",
		"data": map[string]interface{}{
			"client_id":    clientID,
			"display_name": displayName,
		},
	})
	room.mu.RUnlock()

	// Forward existing room tracks to the new peer
	// Done after initial offer/answer to avoid OnNegotiationNeeded glare
	room.mu.RLock()
	existingPeers := make([]*Peer, 0, len(room.Peers))
	for _, p := range room.Peers {
		existingPeers = append(existingPeers, p)
	}
	room.mu.RUnlock()

	for _, existing := range existingPeers {
		if existing.ID == clientID {
			continue
		}
		if existing.remoteAudio != nil {
			newTrack, trackErr := webrtc.NewTrackLocalStaticRTP(
				existing.remoteAudio.Codec().RTPCodecCapability,
				existing.remoteAudio.ID(),
				existing.ID,
			)
			if trackErr == nil {
				sender, addErr := pc.AddTrack(newTrack)
				if addErr == nil {
					log.Printf("[SFU] Added existing audio track from %s to new peer %s", existing.ID, clientID)
					go pumpRemoteToLocal(existing.remoteAudio, newTrack, sender)
				}
			}
		}
		if room.RoomType == "video" && existing.remoteVideo != nil {
			newTrack, trackErr := webrtc.NewTrackLocalStaticRTP(
				existing.remoteVideo.Codec().RTPCodecCapability,
				existing.remoteVideo.ID(),
				existing.ID,
			)
			if trackErr == nil {
				sender, addErr := pc.AddTrack(newTrack)
				if addErr == nil {
					log.Printf("[SFU] Added existing video track from %s to new peer %s", existing.ID, clientID)
					go pumpRemoteToLocal(existing.remoteVideo, newTrack, sender)
				}
			}
		}
	}

	// Start heartbeat
	s.startHeartbeat(roomID, peer)

	return nil
}

// ============================================================
// Errors
// ============================================================

var ErrorRoomFull = &RoomFullError{}

type RoomFullError struct{}

func (e *RoomFullError) Error() string {
	return "room is full (max 4 peers)"
}
