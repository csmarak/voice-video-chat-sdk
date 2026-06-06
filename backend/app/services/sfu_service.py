import asyncio
from typing import Dict, List, Any
import json
import logging
from aiortc import RTCPeerConnection, RTCSessionDescription, MediaStreamTrack
from aiortc.contrib.media import MediaRelay
from starlette.websockets import WebSocketDisconnect

logger = logging.getLogger(__name__)


async def safe_send_text(websocket: Any, message: dict) -> bool:
    try:
        await websocket.send_text(json.dumps(message))
        return True
    except (WebSocketDisconnect, RuntimeError, Exception) as e:
        logger.debug(f"WebSocket send failed: {type(e).__name__}: {e}")
        return False


class PeerContext:
    def __init__(self, client_id: str, display_name: str, websocket: Any):
        self.client_id = client_id
        self.display_name = display_name
        self.websocket = websocket
        self.pc = RTCPeerConnection()
        self.is_muted = False
        self.is_camera_off = False
        self._reneg_task = None


class Room:
    def __init__(self, room_id: str, room_type: str = "voice"):
        self.room_id = room_id
        self.room_type = room_type
        self.peers: Dict[str, PeerContext] = {}
        self.audio_tracks: Dict[str, MediaStreamTrack] = {}
        self.video_tracks: Dict[str, MediaStreamTrack] = {}
        self._reneg_lock = asyncio.Lock()
        self._reneg_generation = 0


class SFUService:
    def __init__(self):
        self.rooms: Dict[str, Room] = {}
        self.relay = MediaRelay()

    def get_or_create_room(self, room_id: str, room_type: str) -> Room:
        if room_id not in self.rooms:
            self.rooms[room_id] = Room(room_id, room_type)
            logger.info(f"Created new {room_type} room: {room_id}")
        return self.rooms[room_id]

    def _get_peer_tracks(self, room: Room, exclude_client_id: str) -> List[MediaStreamTrack]:
        tracks = []
        for peer_id, track in room.audio_tracks.items():
            if peer_id != exclude_client_id:
                tracks.append(self.relay.subscribe(track))
        for peer_id, track in room.video_tracks.items():
            if peer_id != exclude_client_id and room.room_type == "video":
                tracks.append(self.relay.subscribe(track))
        return tracks

    async def _rebuild_single_peer(self, room: Room, peer_ctx: PeerContext, generation: int):
        client_id = peer_ctx.client_id

        if client_id not in room.peers:
            return
        if room._reneg_generation != generation:
            return

        tracks = self._get_peer_tracks(room, client_id)
        if not tracks:
            logger.info(f"No other tracks for {client_id}, skipping rebuild")
            return

        old_pc = peer_ctx.pc
        old_pc_id = id(old_pc)

        new_pc = RTCPeerConnection()
        for track in tracks:
            new_pc.addTrack(track)

        @new_pc.on("connectionstatechange")
        async def on_connectionstatechange():
            state = new_pc.connectionState
            logger.info(f"Client {client_id} WebRTC state: {state}")
            if state in ("failed", "closed"):
                if client_id in room.peers and room.peers[client_id].pc is new_pc:
                    await self.remove_peer(room.room_id, client_id)

        peer_ctx.pc = new_pc

        try:
            await old_pc.close()
        except Exception:
            pass
        await asyncio.sleep(0.05)

        if client_id not in room.peers:
            return
        if room._reneg_generation != generation:
            return

        offer = await new_pc.createOffer()
        await new_pc.setLocalDescription(offer)

        sent = await safe_send_text(peer_ctx.websocket, {
            "type": "server_offer",
            "data": {
                "sdp": new_pc.localDescription.sdp,
                "type": new_pc.localDescription.type
            }
        })

        if sent:
            logger.info(f"Sent renegotiation offer to {client_id} ({len(tracks)} tracks)")
        else:
            logger.info(f"Could not send renegotiation to {client_id} (websocket closed)")

    async def _renegotiate_all(self, room: Room, origin_client_id: str):
        async with room._reneg_lock:
            room._reneg_generation += 1
            generation = room._reneg_generation

            peers_snapshot = list(room.peers.items())
            tasks = []

            for client_id, peer_ctx in peers_snapshot:
                if client_id == origin_client_id:
                    continue
                if client_id not in room.peers:
                    continue
                tasks.append(self._rebuild_single_peer(room, peer_ctx, generation))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def handle_join(self, room_id: str, room_type: str, client_id: str, display_name: str, websocket: Any):
        if room_id in self.rooms and self.rooms[room_id].room_type != room_type:
            room_type = self.rooms[room_id].room_type
            logger.warning(f"Overriding client {client_id} room_type to match existing room format: {room_type}")

        room = self.get_or_create_room(room_id, room_type)

        if len(room.peers) >= 4:
            await safe_send_text(websocket, {"type": "error", "message": f"{room.room_type.capitalize()} room full (Max 4 peers)"})
            await websocket.close(code=4001)
            return None

        peer_ctx = PeerContext(client_id, display_name, websocket)
        room.peers[client_id] = peer_ctx

        @peer_ctx.pc.on("connectionstatechange")
        async def on_connectionstatechange():
            state = peer_ctx.pc.connectionState
            logger.info(f"Client {client_id} WebRTC state: {state}")
            if state in ("failed", "closed"):
                await self.remove_peer(room_id, client_id)

        @peer_ctx.pc.on("track")
        def on_track(track):
            logger.info(f"Received track '{track.kind}' from client {client_id}")
            if track.kind == "audio":
                room.audio_tracks.setdefault(client_id, track)
            elif track.kind == "video" and room.room_type == "video":
                room.video_tracks.setdefault(client_id, track)

            if peer_ctx._reneg_task and not peer_ctx._reneg_task.done():
                peer_ctx._reneg_task.cancel()
                peer_ctx._reneg_task = None

            async def deferred_reneg():
                await asyncio.sleep(0.8)
                if client_id in room.peers:
                    await self._renegotiate_all(room, client_id)

            peer_ctx._reneg_task = asyncio.create_task(deferred_reneg())

        existing_users = [
            {"client_id": p.client_id, "display_name": p.display_name, "is_muted": p.is_muted}
            for p in room.peers.values() if p.client_id != client_id
        ]
        await safe_send_text(websocket, {
            "type": "room_state",
            "data": {"users": existing_users, "room_type": room.room_type}
        })

        await self.broadcast_to_room(room, client_id, {
            "type": "user_joined",
            "data": {"client_id": client_id, "display_name": display_name}
        })

        return peer_ctx

    async def process_offer(self, room_id: str, client_id: str, offer_dict: dict) -> dict:
        room = self.rooms.get(room_id)
        if not room:
            raise ValueError("Room not found")

        peer_ctx = room.peers.get(client_id)
        pc = peer_ctx.pc

        for track in self._get_peer_tracks(room, client_id):
            pc.addTrack(track)

        offer = RTCSessionDescription(sdp=offer_dict["sdp"], type=offer_dict["type"])
        await pc.setRemoteDescription(offer)

        answer = await pc.createAnswer()
        await pc.setLocalDescription(answer)

        return {"sdp": pc.localDescription.sdp, "type": pc.localDescription.type}

    async def process_client_answer(self, room_id: str, client_id: str, answer_dict: dict):
        room = self.rooms.get(room_id)
        if not room or client_id not in room.peers:
            return

        pc = room.peers[client_id].pc
        if pc.signalingState not in ("have-local-offer", "stable"):
            logger.warning(f"Ignoring client_answer for {client_id}, signalingState is {pc.signalingState}")
            return

        try:
            answer = RTCSessionDescription(sdp=answer_dict["sdp"], type=answer_dict["type"])
            await pc.setRemoteDescription(answer)
        except Exception as e:
            logger.exception(f"Failed to set remote answer for client {client_id}: {e}")

    async def broadcast_to_room(self, room: Room, sender_id: str, payload: dict):
        peers_snapshot = list(room.peers.items())
        for client_id, peer in peers_snapshot:
            if client_id != sender_id:
                if client_id not in room.peers:
                    continue
                await safe_send_text(peer.websocket, payload)

    async def remove_peer(self, room_id: str, client_id: str):
        if room_id not in self.rooms:
            return
        room = self.rooms[room_id]

        if client_id not in room.peers:
            return

        peer_ctx = room.peers.pop(client_id)

        if peer_ctx._reneg_task and not peer_ctx._reneg_task.done():
            peer_ctx._reneg_task.cancel()
            peer_ctx._reneg_task = None

        try:
            await peer_ctx.pc.close()
        except Exception:
            pass
        await asyncio.sleep(0.05)

        room.audio_tracks.pop(client_id, None)
        room.video_tracks.pop(client_id, None)

        await self.broadcast_to_room(room, client_id, {
            "type": "user_left",
            "data": {"client_id": client_id}
        })
        logger.info(f"Client {client_id} disconnected entirely.")

        async with room._reneg_lock:
            room._reneg_generation += 1

        if not room.peers:
            self.rooms.pop(room_id, None)
            logger.info(f"Room {room_id} empty. Teardown complete.")


sfu_service = SFUService()
