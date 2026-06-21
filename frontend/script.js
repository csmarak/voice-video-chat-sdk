// ============================================================
// VoiceVidChat — LiveKit-Powered Client
// ============================================================

const SERVER = window.location.origin;
let roomType = 'voice';
let roomCode = '';
let myIdentity = '';
let myDisplayName = '';

let livekitRoom = null;
let localStream = null;
let isMicOn = true;
let isCamOn = false;
let connected = false;

let participants = {};
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
        myIdentity = username + '-' + Math.random().toString(36).slice(2, 8);

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

        await connectToLiveKit();
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
        myIdentity = username + '-' + Math.random().toString(36).slice(2, 8);

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

        await connectToLiveKit();
    } catch (e) {
        toast(e.message, true);
    }
});

// ============================================================
// LiveKit Connection
// ============================================================

async function connectToLiveKit() {
    try {
        const constraints = roomType === 'video'
            ? { audio: true, video: { width: { ideal: 640 }, height: { ideal: 480 } } }
            : { audio: true, video: false };
        localStream = await navigator.mediaDevices.getUserMedia(constraints);
    } catch (e) {
        toast('Camera/Microphone access denied', true);
        return;
    }

    // Get LiveKit token from our server
    let livekitURL = 'ws://localhost:7880';
    let token = '';
    try {
        const tokenRes = await fetch(`${SERVER}/api/livekit/token`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                code: roomCode,
                identity: myIdentity,
                display_name: myDisplayName,
            })
        });
        const tokenData = await tokenRes.json();
        if (!tokenRes.ok) throw new Error(tokenData.error || 'Failed to get token');
        token = tokenData.token;
        if (tokenData.url) livekitURL = tokenData.url;
    } catch (e) {
        toast('Failed to get join token: ' + e.message, true);
        return;
    }

    // Detect if we're on HTTPS — can't use ws:// from HTTPS page
    const isHTTPS = window.location.protocol === 'https:';
    if (isHTTPS && livekitURL.startsWith('ws://')) {
        toast('For phone testing: open http://' + window.location.hostname + ':8081 instead of ngrok URL', true);
    }

    // Connect to LiveKit
    livekitRoom = new LivekitClient.Room({
        adaptiveStream: true,
        dynacast: true,
    });

    livekitRoom.on('participantConnected', (participant) => {
        console.log('Participant connected:', participant.identity);
        participants[participant.identity] = {
            displayName: participant.name || participant.identity,
            isMuted: false,
            audioEl: null,
            videoEl: null,
        };
        renderGrid();

        // tracks are subscribed after participantConnected fires,
        // handled by the trackSubscribed listener below
        participant.on('trackSubscribed', (track) => {
            handleTrackSubscribed(track, participant);
        });
        participant.on('trackUnsubscribed', (track) => {
            handleTrackUnsubscribed(track, participant);
        });
        participant.on('trackMuted', (track) => {
            if (track.kind === 'audio' && participants[participant.identity]) {
                participants[participant.identity].isMuted = true;
                renderGrid();
            }
        });
        participant.on('trackUnmuted', (track) => {
            if (track.kind === 'audio' && participants[participant.identity]) {
                participants[participant.identity].isMuted = false;
                renderGrid();
            }
        });
    });

    livekitRoom.on('participantDisconnected', (participant) => {
        console.log('Participant disconnected:', participant.identity);
        if (participants[participant.identity]) {
            if (participants[participant.identity].audioEl) {
                participants[participant.identity].audioEl.remove();
            }
            delete participants[participant.identity];
        }
        renderGrid();
    });

    livekitRoom.on('connected', () => {
        console.log('Connected to LiveKit room');
        connected = true;
        updateRoomControls();
        startTimer();

        // Publish local tracks
        localStream.getAudioTracks().forEach(t => {
            livekitRoom.localParticipant.publishTrack(t, { source: LivekitClient.Track.Source.Microphone });
        });
        if (roomType === 'video') {
            localStream.getVideoTracks().forEach(t => {
                livekitRoom.localParticipant.publishTrack(t, { source: LivekitClient.Track.Source.Camera });
            });
        }

        // Show self tile
        renderGrid();
    });

    livekitRoom.on('disconnected', () => {
        console.log('Disconnected from LiveKit');
        toast('Disconnected from room', true);
        resetRoom();
    });

    try {
        await livekitRoom.connect(livekitURL, token);
    } catch (e) {
        toast('Failed to connect: ' + e.message, true);
        resetRoom();
    }
}

function handleTrackSubscribed(track, participant) {
    if (!participants[participant.identity]) {
        participants[participant.identity] = {
            displayName: participant.name || participant.identity,
            isMuted: false,
            audioEl: null,
            videoEl: null,
        };
    }

    // LiveKit SDK tracks are RemoteTrack objects — use .mediaStreamTrack
    const mediaStreamTrack = track.mediaStreamTrack || track;

    if (mediaStreamTrack.kind === 'audio') {
        const audioEl = document.createElement('audio');
        audioEl.srcObject = new MediaStream([mediaStreamTrack]);
        audioEl.autoplay = true;
        audioEl.style.display = 'none';
        document.body.appendChild(audioEl);
        // Explicitly play to bypass browser autoplay restrictions
        audioEl.play().catch(e => console.warn('Audio play failed:', e.message));
        participants[participant.identity].audioEl = audioEl;
    } else if (mediaStreamTrack.kind === 'video') {
        const stream = new MediaStream([mediaStreamTrack]);
        participants[participant.identity].videoEl = stream;
    }
    renderGrid();
}

function handleTrackUnsubscribed(track, participant) {
    if (track.kind === 'audio' && participants[participant.identity]) {
        if (participants[participant.identity].audioEl) {
            participants[participant.identity].audioEl.remove();
        }
        participants[participant.identity].audioEl = null;
    } else if (track.kind === 'video') {
        if (participants[participant.identity]) {
            participants[participant.identity].videoEl = null;
        }
    }
    renderGrid();
}

// ============================================================
// Participant Grid
// ============================================================

function renderGrid() {
    const grid = document.getElementById('participantGrid');
    grid.innerHTML = '';

    // Self tile
    const selfTile = createTile(myIdentity, myDisplayName, true, isMicOn, isCamOn);
    grid.appendChild(selfTile);
    if (isCamOn && localStream) {
        setTimeout(() => {
            const selfVideo = selfTile.querySelector('video');
            if (selfVideo) selfVideo.srcObject = localStream;
        }, 50);
    }

    // Peer tiles
    Object.entries(participants).forEach(([id, p]) => {
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
    if (livekitRoom && livekitRoom.localParticipant) {
        livekitRoom.localParticipant.setMicrophoneEnabled(isMicOn);
    }
    if (localStream) {
        localStream.getAudioTracks().forEach(t => t.enabled = isMicOn);
    }
    updateRoomControls();
    renderGrid();
});

document.getElementById('roomCamBtn').addEventListener('click', () => {
    isCamOn = !isCamOn;
    if (livekitRoom && livekitRoom.localParticipant) {
        livekitRoom.localParticipant.setCameraEnabled(isCamOn);
    }
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
    if (livekitRoom) {
        livekitRoom.disconnect();
    }
    resetRoom();
    showView('lobbyView');
}

function resetRoom() {
    connected = false;
    stopTimer();
    if (livekitRoom) { livekitRoom = null; }
    if (localStream) { localStream.getTracks().forEach(t => t.stop()); localStream = null; }
    Object.values(participants).forEach(p => { if (p.audioEl) { p.audioEl.srcObject = null; p.audioEl.remove(); } });
    participants = {};
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
