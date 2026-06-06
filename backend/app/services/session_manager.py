import secrets
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class Session:
    """Represents a meeting session"""
    session_id: str
    room_type: str  # "voice" or "video"
    created_at: datetime
    expires_at: datetime
    created_by: str  # username of creator
    max_peers: int = field(default_factory=lambda: 4)  # default for voice, 2 for video
    active_peers: int = field(default=0)
    
    def is_expired(self) -> bool:
        return datetime.now() > self.expires_at
    
    def is_full(self) -> bool:
        return self.active_peers >= self.max_peers


class SessionManager:
    """Manages meeting sessions"""
    
    def __init__(self, session_ttl_minutes: int = 60):
        self.sessions: Dict[str, Session] = {}
        self.session_ttl_minutes = session_ttl_minutes
    
    def create_session(self, room_type: str, created_by: str) -> str:
        """
        Create a new session and return the session ID.
        
        Args:
            room_type: "voice" (max 4) or "video" (max 4)
            created_by: username of the session creator
            
        Returns:
            session_id: secure 12-character alphanumeric ID
        """
        # Generate secure session ID (12 chars, alphanumeric)
        session_id = secrets.token_urlsafe(9)[:12].replace("_", "A").replace("-", "B")
        
        max_peers = 4
        
        session = Session(
            session_id=session_id,
            room_type=room_type,
            created_at=datetime.now(),
            expires_at=datetime.now() + timedelta(minutes=self.session_ttl_minutes),
            created_by=created_by,
            max_peers=max_peers,
            active_peers=1  # Creator counts as first peer
        )
        
        self.sessions[session_id] = session
        logger.info(f"Session created: {session_id} ({room_type}, created by {created_by})")
        
        return session_id
    
    def get_session(self, session_id: str) -> Optional[Session]:
        """Get a session by ID. Returns None if expired or not found."""
        session = self.sessions.get(session_id)
        
        if not session:
            return None
        
        if session.is_expired():
            self.sessions.pop(session_id, None)
            logger.info(f"Session {session_id} expired and cleaned up")
            return None
        
        return session
    
    def join_session(self, session_id: str) -> bool:
        """
        Attempt to join a session.
        
        Returns:
            True if successful, False if room full or expired
        """
        session = self.get_session(session_id)
        
        if not session:
            return False
        
        if session.is_full():
            logger.warning(f"Session {session_id} is full ({session.active_peers}/{session.max_peers})")
            return False
        
        session.active_peers += 1
        logger.info(f"Peer joined session {session_id} ({session.active_peers}/{session.max_peers})")
        
        return True
    
    def leave_session(self, session_id: str) -> None:
        """Decrement peer count when leaving."""
        session = self.sessions.get(session_id)
        
        if session:
            session.active_peers = max(0, session.active_peers - 1)
            
            # Clean up empty sessions
            if session.active_peers == 0:
                self.sessions.pop(session_id, None)
                logger.info(f"Session {session_id} deleted (no active peers)")
            else:
                logger.info(f"Peer left session {session_id} ({session.active_peers} remaining)")
    
    def cleanup_expired_sessions(self) -> int:
        """Remove all expired sessions. Returns count of removed sessions."""
        expired = [sid for sid, session in self.sessions.items() if session.is_expired()]
        
        for sid in expired:
            self.sessions.pop(sid, None)
        
        if expired:
            logger.info(f"Cleaned up {len(expired)} expired sessions")
        
        return len(expired)


# Global session manager instance
session_manager = SessionManager(session_ttl_minutes=60)
