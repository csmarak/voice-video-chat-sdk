# Voice SDK - WebRTC SFU MVP

## Overview

A voice communication SDK implementing a Selective Forwarding Unit (SFU) architecture for multi-party voice chat using WebRTC.

## Tech Stack

- **Backend**: FastAPI (Python) + aiortc        aiortc : open source python library used for web real time comm and object real time comm(webrtc and ortc), is built on top of asyncio, python's standard aysncrhonous I/0 framework, 
- **Frontend**: HTML/JavaScript
- **Protocol**: WebRTC with WebSocket signaling
- **Dependencies**: FastAPI, Uvicorn, Pydantic, python-dotenv, websockets, aiortc

## Project Structure

```
├── backend/              # FastAPI server
│   ├── main.py          # App initialization & routes setup
│   ├── config/          # Configuration settings
│   ├── routes/          # WebSocket signaling endpoints
│   ├── services/        # SFU service (room & peer management)
│   └── models/          # Data schemas
├── testclient.html      # Client test interface
└── requirements.txt     # Python dependencies
```

## Key Features

- **Voice Rooms**: Multiple users join named rooms
- **SFU Architecture**: Server relays audio tracks between peers (not direct P2P)
- **WebSocket Signaling**: Real-time SDP offer/answer exchange
- **Dynamic Peer Management**: Automatic room cleanup when empty
- **CORS Enabled**: Ready for cross-origin requests

## How It Works

1. Client joins WebSocket: `/ws/voice/room/{room_id}/client/{client_id}`
2. Client sends WebRTC offer
3. Server creates RTCPeerConnection & generates answer
4. Audio tracks are relayed through server to other peers
5. On disconnect, peer is removed and room cleaned up

## API Endpoints

- `GET /health` - Health check
- `WebSocket /ws/voice/room/{room_id}/client/{client_id}` - Signaling
