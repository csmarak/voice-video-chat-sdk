// Global variables
let roomID = "test-room";
let clientID = "user-" + Math.floor(Math.random() * 100000);
let ws = null;
let pc = null;
let localStream = null;
let isConnected = false;
let timerInterval = null;
let elapsedSeconds = 0;

// DOM Elements
const joinBtn = document.getElementById('joinBtn');
const leaveBtn = document.getElementById('leaveBtn');
const micBtn = document.getElementById('micBtn');
const cameraBtn = document.getElementById('cameraBtn');
const settingsBtn = document.getElementById('settingsBtn');
const userIdElement = document.getElementById('userId');
const userStatus = document.getElementById('userStatus');
const controlButtons = document.getElementById('controlButtons');
const topSection = document.querySelector('.top-section');
const timerElement = document.getElementById('timer');

// State management
let isMicOn = true;
let isCameraOn = false;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    userIdElement.textContent = clientID.substring(5, 10).toUpperCase();

    // Event listeners
    joinBtn.addEventListener('click', joinRoom);
    leaveBtn.addEventListener('click', leaveRoom);
    micBtn.addEventListener('click', toggleMic);
    cameraBtn.addEventListener('click', toggleCamera);
    settingsBtn.addEventListener('click', openSettings);
});

// Join Room
async function joinRoom() {
    try {
        joinBtn.disabled = true;
        userStatus.textContent = "Connecting...";

        // Get audio stream from microphone
        try {
            localStream = await navigator.mediaDevices.getUserMedia({ audio: true, video: false });
            console.log("Local audio stream acquired");
        } catch (error) {
            console.error("Error accessing microphone:", error);
            userStatus.textContent = "Microphone Access Denied";
            joinBtn.disabled = false;
            return;
        }

        // Connect to WebSocket signaling server
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.hostname}:8080/ws/voice/room/${roomID}/client/${clientID}`;

        ws = new WebSocket(wsUrl);

        ws.onopen = async () => {
            console.log("Connected to signaling server");

            try {
                // Create RTCPeerConnection
                pc = new RTCPeerConnection();
                console.log("RTCPeerConnection created");

                // Add local audio tracks to peer connection
                localStream.getTracks().forEach(track => {
                    pc.addTrack(track, localStream);
                    console.log("Track added:", track.kind);
                });

                // Handle incoming audio tracks from other peers
                pc.ontrack = (event) => {
                    console.log("Remote track received:", event.track.kind);
                    const audioEl = document.createElement('audio');
                    audioEl.srcObject = event.streams[0];
                    audioEl.autoplay = true;
                    audioEl.controls = false;
                    audioEl.style.display = 'none';
                    document.body.appendChild(audioEl);

                    addParticipant(`peer-${Math.random().toString(36).substr(2, 9)}`);
                };

                // Create and send offer
                const offer = await pc.createOffer();
                await pc.setLocalDescription(offer);
                console.log("Offer created and set as local description");

                ws.send(JSON.stringify({
                    type: 'offer',
                    data: {
                        sdp: pc.localDescription.sdp,
                        type: pc.localDescription.type
                    }
                }));
                console.log("Offer sent to server");

            } catch (error) {
                console.error("Error setting up peer connection:", error);
                userStatus.textContent = "Error Setting Up Connection";
                if (ws) ws.close();
            }
        };

        ws.onmessage = async (event) => {
            try {
                const message = JSON.parse(event.data);
                console.log("Message received:", message.type);

                if (message.type === 'answer' && pc) {
                    const answer = new RTCSessionDescription(message.data);
                    await pc.setRemoteDescription(answer);
                    console.log("Answer received and set as remote description");

                    isConnected = true;
                    userStatus.textContent = "Connected";

                    controlButtons.style.display = 'flex';
                    topSection.classList.add('active');
                    joinBtn.style.display = 'none';
                    leaveBtn.style.display = 'block';

                    startTimer();
                }
                else if (message.type === 'server_offer' && pc) {
                    console.log("Server renegotiation offer received");
                    const offer = new RTCSessionDescription(message.data);
                    await pc.setRemoteDescription(offer);

                    const answer = await pc.createAnswer();
                    await pc.setLocalDescription(answer);

                    ws.send(JSON.stringify({
                        type: 'client_answer',
                        data: {
                            sdp: pc.localDescription.sdp,
                            type: pc.localDescription.type
                        }
                    }));
                    console.log("Renegotiation answer sent");
                }
                else if (message.type === 'room_state') {
                    console.log("Room state:", message.data);
                }
                else if (message.type === 'user_joined') {
                    console.log("User joined:", message.data.client_id);
                }
                else if (message.type === 'user_left') {
                    console.log("User left:", message.data.client_id);
                }
            } catch (error) {
                console.error("Error handling message:", error);
            }
        };

        ws.onerror = (error) => {
            console.error("WebSocket error:", error);
            userStatus.textContent = "Connection Error";
        };

        ws.onclose = () => {
            console.log("Disconnected from signaling server");
            resetUI();
        };

    } catch (error) {
        console.error("Error joining room:", error);
        userStatus.textContent = "Error Connecting";
        joinBtn.disabled = false;
    }
}

// Leave Room
async function leaveRoom() {
    try {
        stopTimer();

        if (pc) {
            pc.close();
            pc = null;
            console.log("Peer connection closed");
        }

        if (localStream) {
            localStream.getTracks().forEach(track => {
                track.stop();
            });
            localStream = null;
            console.log("Local stream stopped");
        }

        if (ws) {
            ws.close();
            ws = null;
        }

        resetUI();
    } catch (error) {
        console.error("Error leaving room:", error);
        resetUI();
    }
}

// Reset UI
function resetUI() {
    isConnected = false;
    userStatus.textContent = "Not Connected";
    controlButtons.style.display = 'none';
    topSection.classList.remove('active');
    joinBtn.style.display = 'block';
    leaveBtn.style.display = 'none';
    joinBtn.disabled = false;

    elapsedSeconds = 0;
    timerElement.textContent = '0:00';

    isMicOn = true;
    isCameraOn = false;

    micBtn.classList.add('active');
    cameraBtn.classList.remove('active');

    console.log("UI reset");
}

// Toggle Microphone
function toggleMic() {
    if (!isConnected) return;

    isMicOn = !isMicOn;

    if (isMicOn) {
        micBtn.classList.add('active');
        console.log("Microphone ON");
    } else {
        micBtn.classList.remove('active');
        console.log("Microphone OFF");
    }
}

// Toggle Camera
function toggleCamera() {
    if (!isConnected) return;

    isCameraOn = !isCameraOn;

    if (isCameraOn) {
        cameraBtn.classList.add('active');
        console.log("Camera ON");
    } else {
        cameraBtn.classList.remove('active');
        console.log("Camera OFF");
    }
}

// Toggle Screen Share
function toggleScreenshare() {
    if (!isConnected) return;
    console.log("Screen share clicked");
}

// Open Settings
function openSettings() {
    if (!isConnected) return;

    alert("Settings menu coming soon!");
    console.log("Settings clicked");
}

// Timer Functions
function startTimer() {
    elapsedSeconds = 0;
    timerInterval = setInterval(() => {
        elapsedSeconds++;
        const minutes = Math.floor(elapsedSeconds / 60);
        const seconds = elapsedSeconds % 60;
        timerElement.textContent = `${minutes}:${seconds.toString().padStart(2, '0')}`;
    }, 1000);
}

function stopTimer() {
    if (timerInterval) {
        clearInterval(timerInterval);
        timerInterval = null;
    }
}

function addParticipant(participantId) {
    console.log("Participant added:", participantId);
}

function removeParticipant(participantId) {
    console.log("Participant removed:", participantId);
}
