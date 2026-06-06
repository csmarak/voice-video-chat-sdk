from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.services.sfu_service import sfu_service, safe_send_text
from app.services.session_manager import session_manager
import json
import asyncio
import logging

router = APIRouter(prefix="/ws/voice", tags=["WebRTC Multi-peer Signaling"])
logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 10
HEARTBEAT_TIMEOUT = 30


@router.websocket("/room/{session_id}/client/{client_id}")
async def signaling_endpoint(
    websocket: WebSocket,
    session_id: str,
    client_id: str,
    name: str = "Anonymous"
):
    await websocket.accept()

    session = session_manager.get_session(session_id)
    if not session:
        await safe_send_text(websocket, {
            "type": "error",
            "message": "Session not found or expired"
        })
        await websocket.close(code=4000)
        return

    peer_ctx = await sfu_service.handle_join(
        session_id,
        session.room_type,
        client_id,
        name,
        websocket
    )

    if not peer_ctx:
        return

    last_heartbeat = asyncio.get_event_loop().time()
    running = True

    async def heartbeat_sender():
        nonlocal running, last_heartbeat
        while running:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            if running:
                sent = await safe_send_text(websocket, {"type": "ping"})
                if not sent:
                    running = False
                    break

    async def heartbeat_monitor():
        nonlocal running, last_heartbeat
        while running:
            await asyncio.sleep(5)
            if running and asyncio.get_event_loop().time() - last_heartbeat > HEARTBEAT_TIMEOUT:
                logger.warning(f"Client {client_id} heartbeat timeout")
                running = False

    heartbeat_task = asyncio.create_task(heartbeat_sender())
    monitor_task = asyncio.create_task(heartbeat_monitor())

    try:
        while running:
            try:
                raw_data = await asyncio.wait_for(websocket.receive_text(), timeout=HEARTBEAT_INTERVAL + 5)
            except asyncio.TimeoutError:
                continue

            message = json.loads(raw_data)
            msg_type = message.get("type")

            if msg_type == "pong":
                last_heartbeat = asyncio.get_event_loop().time()

            elif msg_type == "offer":
                answer_data = await sfu_service.process_offer(session_id, client_id, message.get("data", {}))
                await safe_send_text(websocket, {
                    "type": "answer",
                    "data": answer_data
                })

            elif msg_type == "client_answer":
                await sfu_service.process_client_answer(session_id, client_id, message.get("data", {}))

            elif msg_type == "toggle_mute":
                peer_ctx.is_muted = message.get("data", {}).get("muted", False)
                room = sfu_service.rooms.get(session_id)
                if room:
                    await sfu_service.broadcast_to_room(room, client_id, {
                        "type": "user_muted",
                        "data": {"client_id": client_id, "muted": peer_ctx.is_muted}
                    })

    except WebSocketDisconnect:
        logger.info(f"Client {client_id} WebSocket disconnected")
    finally:
        running = False
        heartbeat_task.cancel()
        monitor_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        await sfu_service.remove_peer(session_id, client_id)
        session_manager.leave_session(session_id)
