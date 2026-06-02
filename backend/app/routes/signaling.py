from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.services.sfu_service import sfu_service
import json
import logging

router = APIRouter(prefix="/ws/voice", tags=["WebRTC Signaling"])
logger = logging.getLogger(__name__)

@router.websocket("/room/{room_id}/client/{client_id}")
async def signaling_endpoint(websocket: WebSocket, room_id: str, client_id: str):
    await websocket.accept()
    logger.info(f"Client {client_id} joined room {room_id}")
    
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message.get("type") == "offer":
                answer_data = await sfu_service.handle_offer(room_id, client_id, message.get("data"))
                
                await websocket.send_text(json.dumps({
                    "type": "answer",
                    "data": answer_data
                }))
                
            # Phase 2: Handle ICE candidates explicitly if aiortc auto-bundling isn't sufficient
            elif message.get("type") == "candidate":
                pass 

    except WebSocketDisconnect:
        logger.info(f"Client {client_id} disconnected from room {room_id}")
        await sfu_service.remove_peer(room_id, client_id)