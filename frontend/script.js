// ============================================================
// VoiceVidChat — Full Lobby + Room Client
// ============================================================

const SERVER = `http://localhost:8081`;
let roomType = 'voice';
let roomCode = '';
let myClientID = '';
let myDisplayName = '';

let ws = null;
let pc = null;
let localStream = null;
let isMicOn = true;
let isCamOn = false;
let connected = false;

let peers = {}; // clientID -> { displayName, isMuted, audioEl?, videoEl? }
let timerInterval = null;
let elapsedSeconds = 0;

// ============================================================
// Navigation
// ============================================================

function showView(id) {
    document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
    document.getElementById(id).classList.add('active');
}

function toast(msg, err) {
    const t = document.getElementById('toast');
    t.textContent = msg;
    t.className = 'toast ' + (err ? 'toast-err' : 'toast-ok');
    t.classList.add('show');
    setTimeout(() => t.classList.remove('show'), 3000);
}

// ============================================================
// Lobby: Room Type Toggle
// ============================================================

document.querySelectorAll('.type-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.type-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        roomType = btn.dataset.type;
    });
});

// ============================================================
// Lobby: Create Room
// ============================================================

document.getElementById('createBtn').addEventListener('click', async () => {
    const username = document.getElementById('lobbyUsername').value.trim();
    const password = document.getElementById('lobbyPassword').value.trim();
    if (!username) return toast('Please enter your name', true);

    try {
        const res = await fetch(`${SERVER}/api/sessions/create`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ room_type: roomType, username, password })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Failed to create room');

        roomCode = data.code;
        myDisplayName = username;
        myClientID = username + '-' + Math.random().toString(36).slice(2, 8);

        showView('roomView');
        document.getElementById('roomCodeDisplay').textContent = roomCode;
        document.getElementById('roomTypeBadge').textContent = roomType;
        document.getElementById('roomTypeBadge').className = 'room-type-badge badge-' + roomType;

        if (roomType === 'video') {
            document.getElementById('roomCamBtn').style.display = 'flex';
            isCamOn = true;
        } else {
            document.getElementById('roomCamBtn').style.display = 'none';
            isCamOn = false;
        }

        connectWebSocket();
    } catch (e) {
        toast(e.message, true);
    }
});

// ============================================================
// Lobby: Navigate to Join
// ============================================================

document.getElementById('goJoinBtn').addEventListener('click', () => showView('joinView'));
document.getElementById('goCreateBtn').addEventListener('click', () => showView('lobbyView'));

// ============================================================
// Join Existing Room
// ============================================================

document.getElementById('joinBtn').addEventListener('click', async () => {
    const code = document.getElementById('joinCode').value.trim().toUpperCase();
    const username = document.getElementById('joinUsername').value.trim();
    const password = document.getElementById('joinPassword').value.trim();

    if (!code || code.length < 4) return toast('Enter a valid meeting code', true);
    if (!username) return toast('Please enter your name', true);

    try {
        const res = await fetch(`${SERVER}/api/sessions/validate`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ code, password })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Cannot join room');

        roomType = data.room_type;
        roomCode = code;
        myDisplayName = username;
        myClientID = username + '-' + Math.random().toString(36).slice(2, 8);

        const joinRes = await fetch(`${SERVER}/api/sessions/join/${code}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password })
        });
        const joinData = await joinRes.json();
        if (!joinRes.ok) throw new Error(joinData.error || 'Cannot join');

        showView('roomView');
        document.getElementById('roomCodeDisplay').textContent = roomCode;
        document.getElementById('roomTypeBadge').textContent = roomType;
        document.getElementById('roomTypeBadge').className = 'room-type-badge badge-' + roomType;

        if (roomType === 'video') {
            document.getElementById('roomCamBtn').style.display = 'flex';
            isCamOn = true;
        } else {
            document.getElementById('roomCamBtn').style.display = 'none';
            isCamOn = false;
        }

        connectWebSocket();
    } catch (e) {
        toast(e.message, true);
    }
});

// ============================================================
// WebSocket + WebRTC
// ============================================================

async function connectWebSocket() {
    try {
        const constraints = roomType === 'video'
            ? { audio: true, video: { width: { ideal: 640 }, height: { ideal: 480 } } }
            : { audio: true, video: false };
        localStream = await navigator.mediaDevices.getUserMedia(constraints);
    } catch (e) {
        toast('Camera/Microphone access denied', true);
        return;
    }

    const wsUrl = `ws://localhost:8081/ws/room/${roomCode}/client/${myClientID}?display_name=${encodeURIComponent(myDisplayName)}`;
    ws = new WebSocket(wsUrl);

    ws.onopen = async () => {
        pc = new RTCPeerConnection();
        localStream.getTracks().forEach(t => pc.addTrack(t, localStream));

        pc.ontrack = (event) => {
            const stream = event.streams[0];
            if (!stream) return;
            const participantID = stream.id || 'unknown';

            if (!peers[participantID]) {
                peers[participantID] = { displayName: undefined, isMuted: false };
            }

            if (event.track.kind === 'audio') {
                const audioEl = document.createElement('audio');
                audioEl.srcObject = stream;
                audioEl.autoplay = true;
                audioEl.style.display = 'none';
                document.body.appendChild(audioEl);
                peers[participantID].audioEl = audioEl;
            } else if (event.track.kind === 'video') {
                peers[participantID].videoEl = stream;
            }
            renderGrid();
        };

        pc.oniceconnectionstatechange = () => {
            if (pc && (pc.iceConnectionState === 'failed' || pc.iceConnectionState === 'disconnected')) {
                console.warn('ICE state:', pc.iceConnectionState);
            }
        };

        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        ws.send(JSON.stringify({ type: 'offer', data: { sdp: pc.localDescription.sdp, type: 'offer' } }));
    };

    ws.onmessage = async (event) => {
        let msg;
        try { msg = JSON.parse(event.data); } catch { return; }

        switch (msg.type) {
            case 'answer':
                await pc.setRemoteDescription(new RTCSessionDescription(msg.data));
                connected = true;
                updateRoomControls();
                startTimer();
                break;

            case 'server_offer':
                try {
                    await pc.setRemoteDescription(new RTCSessionDescription(msg.data));
                } catch (e) {
                    if (e.name === 'InvalidStateError') {
                        console.warn('server_offer ignored — already in negotiation:', pc.signalingState);
                        break;
                    }
                    throw e;
                }
                const answer = await pc.createAnswer();
                await pc.setLocalDescription(answer);
                ws.send(JSON.stringify({ type: 'client_answer', data: { sdp: pc.localDescription.sdp, type: 'answer' } }));
                break;

            case 'room_state':
                handleRoomState(msg.data);
                break;

            case 'user_joined':
                handleUserJoined(msg.data);
                break;

            case 'user_left':
                handleUserLeft(msg.data);
                break;

            case 'user_muted':
                handleUserMuted(msg.data);
                break;

            case 'ping':
                ws.send(JSON.stringify({ type: 'pong' }));
                break;

            case 'error':
                toast(msg.message || 'Server error', true);
                if (msg.message === 'room is full (max 4 peers)') {
                    setTimeout(leaveRoom, 1000);
                }
                break;
        }
    };

    ws.onerror = () => toast('Connection error', true);
    ws.onclose = (e) => {
        if (connected) {
            toast('Disconnected from room', true);
        }
        resetRoom();
    };
}

// ============================================================
// Room State & Participants
// ============================================================

function handleRoomState(data) {
    const newPeers = {};
    (data.users || []).forEach(u => {
        if (u.client_id !== myClientID) {
            const existing = peers[u.client_id];
            newPeers[u.client_id] = {
                displayName: u.display_name || 'Peer',
                isMuted: u.is_muted || false,
                audioEl: existing ? existing.audioEl : undefined,
                videoEl: existing ? existing.videoEl : undefined,
            };
        }
    });
    peers = newPeers;
    renderGrid();
}

function handleUserJoined(data) {
    if (data.client_id === myClientID) return;
    const existing = peers[data.client_id];
    peers[data.client_id] = {
        displayName: data.display_name || 'Peer',
        isMuted: false,
        audioEl: existing ? existing.audioEl : undefined,
        videoEl: existing ? existing.videoEl : undefined,
    };
    renderGrid();
    toast(`${data.display_name} joined`, false);
}

function handleUserLeft(data) {
    if (peers[data.client_id]) {
        if (peers[data.client_id].audioEl) {
            peers[data.client_id].audioEl.srcObject = null;
            peers[data.client_id].audioEl.remove();
        }
        delete peers[data.client_id];
    }
    renderGrid();
}

function handleUserMuted(data) {
    if (peers[data.client_id]) {
        peers[data.client_id].isMuted = data.muted;
        renderGrid();
    }
}

// ============================================================
// Participant Grid
// ============================================================

function renderGrid() {
    const grid = document.getElementById('participantGrid');
    grid.innerHTML = '';

    // Self tile
    const selfTile = createTile(myClientID, myDisplayName, true, isMicOn, isCamOn);
    grid.appendChild(selfTile);
    if (isCamOn && localStream) {
        setTimeout(() => {
            const selfVideo = selfTile.querySelector('video');
            if (selfVideo) selfVideo.srcObject = localStream;
        }, 50);
    }

    // Peer tiles
    Object.entries(peers).forEach(([id, p]) => {
        const tile = createTile(id, p.displayName, false, !p.isMuted, !!p.videoEl);
        grid.appendChild(tile);
        if (p.videoEl) {
            setTimeout(() => {
                const peerVideo = tile.querySelector('video');
                if (peerVideo) peerVideo.srcObject = p.videoEl;
            }, 50);
        }
    });
}

function createTile(id, name, isSelf, micOn, camOn) {
    const tile = document.createElement('div');
    tile.className = 'participant-tile';
    tile.id = 'tile-' + id;

    if (camOn) {
        const video = document.createElement('video');
        video.autoplay = true;
        video.playsInline = true;
        video.muted = isSelf;
        tile.appendChild(video);
    } else {
        const avatar = document.createElement('div');
        avatar.className = 'participant-avatar';
        avatar.textContent = (name || '?').charAt(0).toUpperCase();
        tile.appendChild(avatar);
    }

    const label = document.createElement('div');
    label.className = 'participant-label';
    label.innerHTML = `
        <span>${name}${isSelf ? ' (You)' : ''}</span>
        ${!micOn ? '<svg class="mic-off-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="1" y1="1" x2="23" y2="23"/><path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"/><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23"/><line x1="12" y1="19" x2="12" y2="23"/></svg>' : ''}
    `;
    tile.appendChild(label);

    return tile;
}

// ============================================================
// Room Controls
// ============================================================

function updateRoomControls() {
    document.getElementById('roomMicBtn').className = 'ctrl-btn' + (isMicOn ? ' active' : '');
    document.getElementById('roomCamBtn').className = 'ctrl-btn' + (isCamOn ? ' active' : '');
}

document.getElementById('roomMicBtn').addEventListener('click', () => {
    isMicOn = !isMicOn;
    if (localStream) {
        localStream.getAudioTracks().forEach(t => t.enabled = isMicOn);
    }
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'toggle_mute', data: { muted: !isMicOn } }));
    }
    updateRoomControls();
    renderGrid();
});

document.getElementById('roomCamBtn').addEventListener('click', () => {
    isCamOn = !isCamOn;
    if (localStream) {
        localStream.getVideoTracks().forEach(t => t.enabled = isCamOn);
    }
    updateRoomControls();
    renderGrid();
});

document.getElementById('roomLeaveBtn').addEventListener('click', leaveRoom);

// ============================================================
// Leave & Reset
// ============================================================

function leaveRoom() {
    stopTimer();
    if (ws) {
        try { ws.close(); } catch {}
    }
    resetRoom();
    showView('lobbyView');
}

function resetRoom() {
    connected = false;
    stopTimer();
    if (pc) { pc.close(); pc = null; }
    if (localStream) { localStream.getTracks().forEach(t => t.stop()); localStream = null; }
    Object.values(peers).forEach(p => { if (p.audioEl) { p.audioEl.srcObject = null; p.audioEl.remove(); } });
    peers = {};
    ws = null;
    document.getElementById('participantGrid').innerHTML = '';
}

// ============================================================
// Timer
// ============================================================

function startTimer() {
    elapsedSeconds = 0;
    document.getElementById('roomTimer').textContent = '0:00';
    timerInterval = setInterval(() => {
        elapsedSeconds++;
        const m = Math.floor(elapsedSeconds / 60);
        const s = elapsedSeconds % 60;
        document.getElementById('roomTimer').textContent = `${m}:${String(s).padStart(2, '0')}`;
    }, 1000);
}

function stopTimer() {
    if (timerInterval) { clearInterval(timerInterval); timerInterval = null; }
}
