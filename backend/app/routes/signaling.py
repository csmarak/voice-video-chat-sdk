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
        await safe_send_text(websocket, {"type": "error", "message": "Session not found or expired"})
        await websocket.close(code=4004, reason="Session not found or expired")
        return

    peer_ctx = await sfu_service.handle_join(session_id, session.room_type, client_id, name, websocket)

    if not peer_ctx:
        return

    last_heartbeat = asyncio.get_event_loop().time()
    running = True
    heartbeat_task = None
    monitor_task = None

    async def heartbeat_sender():
        nonlocal running, last_heartbeat
        while running:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            if not running:
                break
            sent = await safe_send_text(websocket, {"type": "ping"})
            if not sent:
                running = False
                break

    async def heartbeat_monitor():
        nonlocal running, last_heartbeat
        while running:
            await asyncio.sleep(5)
            if not running:
                break
            if asyncio.get_event_loop().time() - last_heartbeat > HEARTBEAT_TIMEOUT:
                logger.warning(f"Client {client_id} heartbeat timeout")
                running = False
                break

    heartbeat_task = asyncio.create_task(heartbeat_sender())
    monitor_task = asyncio.create_task(heartbeat_monitor())

    try:
        while running:
            try:
                raw_data = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=HEARTBEAT_INTERVAL + 5
                )
            except asyncio.TimeoutError:
                continue
            except WebSocketDisconnect:
                break
            except RuntimeError:
                break
            except Exception:
                logger.exception(f"Unexpected error receiving message from {client_id}")
                break

            try:
                message = json.loads(raw_data)
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON from {client_id}")
                continue

            msg_type = message.get("type")

            if msg_type == "pong":
                last_heartbeat = asyncio.get_event_loop().time()

            elif msg_type == "offer":
                try:
                    answer_data = await sfu_service.process_offer(
                        session_id, client_id, message.get("data", {})
                    )
                    await safe_send_text(websocket, {"type": "answer", "data": answer_data})
                except ValueError as e:
                    logger.warning(f"Invalid offer from {client_id}: {e}")
                    await safe_send_text(websocket, {"type": "error", "message": str(e)})
                except RuntimeError as e:
                    logger.warning(f"PC dead for {client_id}, rejecting offer: {e}")
                except Exception:
                    logger.exception(f"Failed to process offer from {client_id}")

            elif msg_type == "client_answer":
                try:
                    await sfu_service.process_client_answer(
                        session_id, client_id, message.get("data", {})
                    )
                except Exception:
                    logger.exception(f"Failed to process answer from {client_id}")

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
    except RuntimeError:
        logger.info(f"Client {client_id} WebSocket runtime error (likely closed)")
    except Exception:
        logger.exception(f"Client {client_id} unhandled exception in signaling loop")
    finally:
        running = False
        if heartbeat_task:
            heartbeat_task.cancel()
        if monitor_task:
            monitor_task.cancel()

        async def cancel_task(t):
            try:
                if t and not t.done():
                    t.cancel()
                    await t
            except (asyncio.CancelledError, Exception):
                pass

        await cancel_task(heartbeat_task)
        await cancel_task(monitor_task)

        await sfu_service.remove_peer(session_id, client_id)
        session_manager.leave_session(session_id)
