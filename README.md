# VoiceVidChat SDK — Project Documentation

**Last Updated:** June 9, 2026
**Status:** MVP (Working)

---

## What It Is

A **Selective Forwarding Unit (SFU)** for multi-party voice/video chat. The server acts as a relay — clients connect to the server, and it forwards audio/video streams between peers. Supports rooms of up to 4 participants with shareable session codes.

---

## Tech Stack

| Layer | Tech |
|---|---|
| Backend | Python, FastAPI, Uvicorn |
| WebRTC | aiortc (Python), browser WebRTC API |
| Signaling | WebSockets |
| Frontend | Vanilla HTML/CSS/JS (no frameworks) |
| NAT Traversal | STUN (Google) + TURN (Metered.ca) — in test2.html |

---

## Architecture

```
Client → WebSocket → FastAPI Server → SFU Service (MediaRelay) → Other Clients
                                     → Session Manager (rooms, cleanup)
```

### Key Components

- **`backend/app/routes/signaling.py`** — WebSocket endpoint for WebRTC SDP exchange
- **`backend/app/routes/sessions.py`** — REST API for create/join/validate rooms
- **`backend/app/services/sfu_service.py`** — Core SFU: peer connections, MediaRelay track forwarding, renegotiation
- **`backend/app/services/session_manager.py`** — Room lifecycle: creation, zombie cleanup (8s grace), 60-min TTL, max 4 peers
- **`frontend/`** — Voice room client (dark theme, responsive)
- **`test2.html`** — Full-featured video+audio client with create/join UI

### API Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/health` | Health check |
| `POST` | `/api/sessions/create` | Create session → returns share code |
| `POST` | `/api/sessions/validate` | Validate session before joining |
| `POST` | `/api/sessions/join/{id}` | Confirm join |
| `WS` | `/ws/voice/room/{sid}/client/{cid}` | Real-time signaling |

---

## What's Working ✅

- SFU audio relay between up to 4 peers using aiortc MediaRelay
- Session create/validate/join flow with 12-char share codes
- Dynamic renegotiation when peers join/leave (server sends new offers)
- Heartbeat ping/pong with 30s timeout
- Zombie room teardown (graceful shutdown when room empties)
- Session expiry & cleanup (60-min TTL)
- Mute toggle broadcasting
- Frontend: responsive dark UI with listening indicator, timer, mic controls
- Video room type supported in backend (test2.html client works with video)

---

## In Progress / Known Gaps 🚧

- **Main frontend (`frontend/index.html`) is audio-only** — doesn't request video from camera
- **Mute state not synced** — toggle goes to server but frontend doesn't listen for `user_muted` events
- **No TURN config in main frontend** — only `test2.html` has NAT traversal fallback
- **Camera/Settings/Screenshare buttons** — stubs, not functional yet
- **No auth** — anyone can join with any name
- **In-memory only** — no database; server restart loses all sessions
- **No TLS** — runs on raw HTTP/WS (port 8080)
- **Video bugs being fixed** (per latest commit)

---

## Project Structure

```
voicevidchat sdk/
├── backend/              # Main backend (current, feature-complete)
│   └── app/
│       ├── main.py
│       ├── routes/       # signaling.py, sessions.py
│       ├── services/     # sfu_service.py, session_manager.py
│       ├── models/       # schema.py
│       └── config/       # config.py
├── backend-backup/       # Older prototype (simpler, no sessions/renegotiation)
├── frontend/             # Voice client (index.html, script.js, style.css)
├── test1.html            # Minimal smoke-test client
├── test2.html            # Full video+audio client with create/join UI
└── README.md             # Detailed architecture docs with diagrams
```

---

## Running

```bash
cd backend
pip install -r requirements.txt
python -m app.main
# Server starts at http://0.0.0.0:8080
```

Open `frontend/index.html` (voice only) or `test2.html` (video+voice) in browser.
