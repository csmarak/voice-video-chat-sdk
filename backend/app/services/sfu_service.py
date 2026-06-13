import asyncio
from typing import Dict, List, Any
import json
import logging
from aiortc import RTCPeerConnection, RTCSessionDescription, MediaStreamTrack, RTCRtpSender
from aiortc.contrib.media import MediaRelay
from aiortc.exceptions import InvalidStateError
from starlette.websockets import WebSocketDisconnect

logger = logging.getLogger(__name__)

# --- AIORTC LIBRARY BUG MONKEY PATCH ---
# Known aiortc __encoder teardown race (GitHub Issue #1124):
# RTCP feedback packets arriving after PC close hit a deleted encoder.
# This intercepts _handle_rtcp_packet and silently swallows the specific AttributeError.
_original_handle_rtcp_packet = RTCRtpSender._handle_rtcp_packet


async def _safe_handle_rtcp_packet(self, packet):
    try:
        if not hasattr(self, "_RTCRtpSender__encoder"):
            return
        await _original_handle_rtcp_packet(self, packet)
    except AttributeError as e:
        if "_RTCRtpSender__encoder" in str(e):
            pass
        else:
            raise e


RTCRtpSender._handle_rtcp_packet = _safe_handle_rtcp_packet


async def safe_send_text(websocket: Any, message: dict) -> bool:
    try:
        await websocket.send_text(json.dumps(message))
        return True
    except (WebSocketDisconnect, RuntimeError, Exception) as e:
        logger.debug(f"WebSocket send failed: {type(e).__name__}: {e}")
        return False


def _spawn_bg_task(coro, peer_ctx: "PeerContext"):
    task = asyncio.create_task(coro)
    peer_ctx._bg_tasks.add(task)
    task.add_done_callback(lambda t: peer_ctx._bg_tasks.discard(t))
    return task


class PeerContext:
    def __init__(self, client_id: str, display_name: str, websocket: Any):
        self.client_id = client_id
        self.display_name = display_name
        self.websocket = websocket
        self.pc = RTCPeerConnection()
        self.is_muted = False
        self.is_camera_off = False
        self._bg_tasks: set = set()
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
        self._is_zombie = False
        self._teardown_task: asyncio.Task = None


class SFUService:
    def __init__(self):
        self.rooms: Dict[str, Room] = {}
        self.relay = MediaRelay()

    def get_or_create_room(self, room_id: str, room_type: str) -> Room:
        if room_id in self.rooms:
            room = self.rooms[room_id]
            if room._teardown_task and not room._teardown_task.done():
                room._teardown_task.cancel()
                room._teardown_task = None
                logger.info(f"Room {room_id} revived from zombie state")
            room._is_zombie = False
            return room
        self.rooms[room_id] = Room(room_id, room_type)
        logger.info(f"Created new {room_type} room: {room_id}")
        return self.rooms[room_id]

    async def _close_pc_safely(self, pc: RTCPeerConnection, owner_id: str):
        pc.on("connectionstatechange")(lambda: None)
        try:
            await pc.close()
        except Exception:
            pass
        await asyncio.sleep(0.1)

    def _pc_is_alive(self, pc: RTCPeerConnection) -> bool:
        try:
            return pc.connectionState not in ("closed", "failed")
        except Exception:
            return False

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

        if peer_ctx.pc.connectionState in ("closed", "failed"):
            return

        tracks = self._get_peer_tracks(room, client_id)
        if not tracks:
            logger.info(f"No other tracks for {client_id}, skipping rebuild")
            return

        old_pc = peer_ctx.pc

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

        for sender in old_pc.getSenders():
            if sender.track:
                sender.track.stop()

        await self._close_pc_safely(old_pc, client_id)

        if client_id not in room.peers:
            return
        if room._reneg_generation != generation:
            return

        try:
            offer = await new_pc.createOffer()
            await new_pc.setLocalDescription(offer)
        except InvalidStateError:
            logger.warning(f"Skipped renegotiation for {client_id}, connection is dead.")
            await self._close_pc_safely(new_pc, client_id)
            return
        except Exception as e:
            logger.exception(f"Failed to create renegotiation offer for {client_id}: {e}")
            await self._close_pc_safely(new_pc, client_id)
            return

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
            await self._close_pc_safely(new_pc, client_id)
            return

    async def _renegotiate_all(self, room: Room, origin_client_id: str):
        try:
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
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for i, r in enumerate(results):
                        if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
                            logger.warning(f"Rebuild task failed silently: {r}")
        except (asyncio.CancelledError, Exception) as e:
            if not isinstance(e, asyncio.CancelledError):
                logger.warning(f"Renegotiation aborted for room {room.room_id}: {e}")

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
                if client_id in room.peers:
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
                peer_ctx._bg_tasks.discard(peer_ctx._reneg_task)
                peer_ctx._reneg_task = None

            async def deferred_reneg():
                try:
                    await asyncio.sleep(0.8)
                    if client_id in room.peers:
                        await self._renegotiate_all(room, client_id)
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.warning(f"Deferred renegotiation failed for {client_id}: {e}")

            peer_ctx._reneg_task = _spawn_bg_task(deferred_reneg(), peer_ctx)

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
        if not peer_ctx:
            raise ValueError("Peer not found in room")

        pc = peer_ctx.pc
        if not self._pc_is_alive(pc):
            raise RuntimeError(f"PC for {client_id} is {pc.connectionState}")

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
        if not self._pc_is_alive(pc):
            logger.info(f"Ignoring client_answer for {client_id}, PC is {pc.connectionState}")
            return

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
        for t in list(peer_ctx._bg_tasks):
            if not t.done():
                t.cancel()
        peer_ctx._bg_tasks.clear()

        for sender in peer_ctx.pc.getSenders():
            if sender.track:
                sender.track.stop()

        await self._close_pc_safely(peer_ctx.pc, client_id)

        room.audio_tracks.pop(client_id, None)
        room.video_tracks.pop(client_id, None)

        await self.broadcast_to_room(room, client_id, {
            "type": "user_left",
            "data": {"client_id": client_id}
        })
        logger.info(f"Client {client_id} disconnected entirely.")

        async with room._reneg_lock:
            room._reneg_generation += 1

        if not room.peers and not room._is_zombie:
            room._is_zombie = True

            async def delayed_teardown():
                await asyncio.sleep(8)
                if room_id in self.rooms and self.rooms[room_id] is room:
                    if not room.peers:
                        self.rooms.pop(room_id, None)
                        logger.info(f"Room {room_id} zombie expired. Teardown complete.")
                    else:
                        logger.info(f"Room {room_id} revived during zombie period, cancelling teardown")
                room._is_zombie = False

            room._teardown_task = asyncio.create_task(delayed_teardown())
            logger.info(f"Room {room_id} empty, entering zombie state (8s teardown)")


sfu_service = SFUService()
