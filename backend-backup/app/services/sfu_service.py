import asyncio
from typing import Dict, List

from aiortc import RTCPeerConnection, RTCSessionDescription, MediaStreamTrack
from aiortc.contrib.media import MediaRelay

class Room:
    def __init__(self, room_id: str):
        self.room_id = room_id
        self.peers: Dict[str, RTCPeerConnection] = {}
        self.tracks: List[MediaStreamTrack] = []
        

class SFUService:
    def __init__(self):
        self.rooms: Dict[str, Room] = {}
        self.relay = MediaRelay()
        
        
    def get_or_create_room(self, room_id: str) -> Room:
        if room_id not in self.rooms:
            self.rooms[room_id] = Room(room_id)
        return self.rooms[room_id]
    
    
    async def handle_offer(self, room_id: str, client_id: str, offer_dict: dict) -> dict:
        room = self.get_or_create_room(room_id)
        pc = RTCPeerConnection()
        
        room.peers[client_id] = pc
        
        
        
        for track in room.tracks:
            pc.addTrack(self.relay.subscribe(track))
            
            
        
        @pc.on("track")
        def on_track(track):
            if track.kind == "audio":
                room.tracks.append(track)
                
                
        @pc.on("connectionstatechange")
        async def on_connectionstatechange():
            if pc.connectionState in ["failed", "closed"]:
                await self.remove_peer(room_id, client_id)
                
                
        
        offer = RTCSessionDescription(sdp=offer_dict["sdp"], type=offer_dict["type"])
        await pc.setRemoteDescription(offer)
        
        answer = await pc.createAnswer()
        await pc.setLocalDescription(answer)
        
        
        
        return {"sdp": pc.localDescription.sdp, "type": pc.localDescription.type}
    
    
    async def remove_peer(self, room_id: str, client_id: str):
        # Be resilient to concurrent removals: fetch the room atomically
        room = self.rooms.get(room_id)
        if not room:
            return

        pc = room.peers.pop(client_id, None)
        if pc:
            try:
                await pc.close()
            except Exception:
                pass

        # If room has no more peers, remove it safely
        if not room.peers:
            self.rooms.pop(room_id, None)
                
sfu_service = SFUService()