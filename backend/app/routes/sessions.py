from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import logging

from app.services.session_manager import session_manager

router = APIRouter(prefix="/api/sessions", tags=["Sessions"])
logger = logging.getLogger(__name__)


class CreateSessionRequest(BaseModel):
    room_type: str  # "voice" or "video"
    username: str


class JoinSessionRequest(BaseModel):
    username: str


class SessionResponse(BaseModel):
    session_id: str
    room_type: str
    max_peers: int
    can_join: bool


@router.post("/create", response_model=dict)
async def create_session(req: CreateSessionRequest):
    """
    Create a new meeting session.
    
    Returns the session ID that can be shared with others.
    """
    if req.room_type not in ["voice", "video"]:
        raise HTTPException(status_code=400, detail="room_type must be 'voice' or 'video'")
    
    if not req.username or len(req.username.strip()) == 0:
        raise HTTPException(status_code=400, detail="username is required")
    
    session_id = session_manager.create_session(req.room_type, req.username)
    
    return {
        "session_id": session_id,
        "room_type": req.room_type,
        "max_peers": 4,
        "share_link": f"Join Code: {session_id}"
    }


@router.post("/validate", response_model=SessionResponse)
async def validate_session(req: JoinSessionRequest, session_id: str):
    """
    Validate if a session exists and can be joined.
    """
    if not session_id or len(session_id.strip()) == 0:
        raise HTTPException(status_code=400, detail="session_id is required")
    
    if not req.username or len(req.username.strip()) == 0:
        raise HTTPException(status_code=400, detail="username is required")
    
    session = session_manager.get_session(session_id)
    
    if not session:
        raise HTTPException(status_code=404, detail="Session not found or expired")
    
    if session.is_full():
        raise HTTPException(status_code=409, detail=f"Session is full ({session.active_peers}/{session.max_peers})")
    
    return SessionResponse(
        session_id=session_id,
        room_type=session.room_type,
        max_peers=session.max_peers,
        can_join=True
    )


@router.post("/join/{session_id}")
async def join_session(session_id: str, req: JoinSessionRequest):
    """
    Confirm joining a session. This should be called before establishing WebSocket.
    """
    if not req.username or len(req.username.strip()) == 0:
        raise HTTPException(status_code=400, detail="username is required")
    
    success = session_manager.join_session(session_id)
    
    if not success:
        raise HTTPException(status_code=409, detail="Cannot join session (full or expired)")
    
    session = session_manager.get_session(session_id)
    
    return {
        "joined": True,
        "session_id": session_id,
        "room_type": session.room_type,
        "message": f"Successfully joined session {session_id}"
    }


@router.get("/cleanup")
async def cleanup_sessions():
    """
    Admin endpoint to clean up expired sessions.
    """
    count = session_manager.cleanup_expired_sessions()
    return {"cleaned_up": count, "active_sessions": len(session_manager.sessions)}
