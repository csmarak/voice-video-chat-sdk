# Session Context — June 13, 2026

## Bugs Fixed

### 1. Phantom "Peer" Users

**Cause:** SFU forwarded tracks with browser's auto-generated stream ID (e.g., `"stream0"`), which didn't match any client ID. Frontend's `ontrack` couldn't map them, created mystery entries named `"Peer"`.

**Fix:**

- `backend-backup/sfu.go` — `forwardTrackToPeer()` now uses `sourceClientID` as stream ID instead of `track.StreamID()`. All three call sites updated: OnTrack forwarding, existing audio forwarding to new peer, existing video forwarding to new peer.
- `frontend/script.js` — `handleRoomState()` preserves existing `audioEl`/`videoEl` instead of resetting `peers = {}`.
- `frontend/script.js` — `ontrack` no longer creates entries with fallback name `"Peer"`. Uses `undefined` initially, filled in by `room_state`/`user_joined`.
- `frontend/script.js` — `handleUserJoined()` preserves existing media elements.
- `frontend/script.js` — `createTile()` handles `undefined` displayName with `'?'` fallback.

### 2. Host Not Receiving Joiner's Tracks

**Cause:** Manual renegotiation state machine had race conditions. Two `OnTrack` calls (audio + video) both saw `isNegotiating == false`, launched parallel `doRenegotiate` calls, second one failed silently.

**Fix:** Replaced entire hand-rolled negotiation system with Pion's native `OnNegotiationNeeded`:

- Removed `renegMu`, `isNegotiating`, `negotiationPending` from `Peer` struct.
- Removed `doRenegotiate()`, `drainRenegotiation()`, `requestRenegotiation()`.
- Added `setupOnNegotiationNeeded(peer)` — registers Pion handler that checks `SignalingStateStable`, creates offer, sends `server_offer`.
- Registered on new peer's PC and all existing peers' PCs (to cover peers created before this fix).
- `OnTrack` now just calls `forwardTrackToPeer` with no renegotiation logic.
- `HandleClientAnswer` simplified to just `SetRemoteDescription` — Pion auto-refires `OnNegotiationNeeded` when state returns to stable.
- `RemovePeer` no longer has reneg cleanup.

### 3. Frontend State Destruction on `room_state`

**Cause:** `handleRoomState()` did `peers = {}` then rebuilt, losing audio/video element references attached by `ontrack`.

**Fix:** Preserves `audioEl` and `videoEl` from existing peer entries when rebuilding.

### 4. `file://` Protocol / Connection Refused / 404

**Cause:** Opening `index.html` directly from disk made `location.protocol = "file:"` and `location.hostname = ""`, producing URLs like `file://:8081/api/sessions/create`.

**Fix:**

- `frontend/script.js` — `SERVER` hardcoded to `http://localhost:8081`.
- `frontend/script.js` — WebSocket URL hardcoded to `ws://localhost:8081/ws/room/...`.
- The 404 in devtools is browser requesting `favicon.ico` — harmless.

## Architecture Notes

### Project Structure

```text
voicevidchat sdk/
├── backend/              # Python/FastAPI SFU (aiortc) — ports 8080
├── backend-backup/       # Go/Pion SFU (active) — port 8081
│   ├── main.go           # Session API, HTTP routes, WebSocket dispatcher
│   └── sfu.go            # Room/peer management, track forwarding, signaling
└── frontend/             # Vanilla HTML/JS/CSS SPA
    ├── index.html
    ├── script.js
    └── style.css
```

### Server

- Go Pion SFU on `:8081`
- Session API: `POST /api/sessions/create`, `validate`, `join/{code}`
- WebSocket signaling: `/ws/room/{code}/client/{clientID}`
- Build: `go build -o server.exe .` from `backend-backup/`
- Run: `./server.exe`

### Signaling Flow

1. Client POSTs `/api/sessions/create` → gets room code
2. Client opens WebSocket to `/ws/room/{code}/client/{clientID}`
3. Client sends `offer` → server sends `answer` + `room_state`
4. On new peer join: server forwards existing tracks to new peer, fires `OnNegotiationNeeded` on existing peers to renegotiate
5. Heartbeat: 10s ping, 30s timeout

### Key SFU Decisions

- `OnNegotiationNeeded` handles all renegotiation — no manual queue
- Track stream IDs set to `sourceClientID` for frontend identification
- Max 4 peers per room
- STUN: `stun.l.google.com:19302`
- No TURN server — requires direct connectivity

## Testing Notes

- Single machine multi-tab testing unreliable due to: camera driver locks, Chrome port exhaustion, shared `localhost` loopback
- Use different devices (phone + laptop on same WiFi) for real-world testing
- Or use Chrome with `--use-fake-device-for-media-stream` flag for multi-tab testing
- Open frontend via `http://<LAN-IP>:8081` for phone testing
