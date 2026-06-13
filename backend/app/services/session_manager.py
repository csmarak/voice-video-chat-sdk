import secrets
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class Session:
    session_id: str
    room_type: str
    created_at: datetime
    expires_at: datetime
    created_by: str
    max_peers: int = field(default_factory=lambda: 4)
    active_peers: int = field(default=0)
    is_zombie: bool = False

    def is_expired(self) -> bool:
        return datetime.now() > self.expires_at

    def is_full(self) -> bool:
        return self.active_peers >= self.max_peers

    def can_reconnect(self) -> bool:
        return self.is_zombie and not self.is_expired()


class SessionManager:
    def __init__(self, session_ttl_minutes: int = 60, zombie_delay_seconds: int = 10):
        self.sessions: Dict[str, Session] = {}
        self.session_ttl_minutes = session_ttl_minutes
        self.zombie_delay_seconds = zombie_delay_seconds

    def create_session(self, room_type: str, created_by: str) -> str:
        session_id = secrets.token_urlsafe(9)[:12].replace("_", "A").replace("-", "B")
        session = Session(
            session_id=session_id,
            room_type=room_type,
            created_at=datetime.now(),
            expires_at=datetime.now() + timedelta(minutes=self.session_ttl_minutes),
            created_by=created_by,
            max_peers=4,
            active_peers=1,
        )
        self.sessions[session_id] = session
        logger.info(f"Session created: {session_id} ({room_type}, created by {created_by})")
        return session_id

    def get_session(self, session_id: str, allow_zombie: bool = True) -> Optional[Session]:
        session = self.sessions.get(session_id)
        if not session:
            return None
        if session.is_expired():
            self.sessions.pop(session_id, None)
            logger.info(f"Session {session_id} expired and cleaned up")
            return None
        return session

    def join_session(self, session_id: str) -> bool:
        session = self.get_session(session_id)
        if not session:
            return False

        if session.is_zombie:
            session.is_zombie = False
            session.active_peers = 1
            logger.info(f"Session {session_id} revived from zombie by reconnection")
            return True

        if session.is_full():
            logger.warning(f"Session {session_id} is full ({session.active_peers}/{session.max_peers})")
            return False

        session.active_peers += 1
        logger.info(f"Peer joined session {session_id} ({session.active_peers}/{session.max_peers})")
        return True

    def leave_session(self, session_id: str) -> None:
        session = self.sessions.get(session_id)
        if not session:
            return

        session.active_peers = max(0, session.active_peers - 1)

        if session.active_peers == 0 and not session.is_zombie:
            session.is_zombie = True
            logger.info(f"Session {session_id} zombie state started ({self.zombie_delay_seconds}s grace)")

            async def delayed_delete():
                await asyncio.sleep(self.zombie_delay_seconds)
                if session_id in self.sessions:
                    sess = self.sessions[session_id]
                    if sess.is_zombie and sess.active_peers == 0:
                        self.sessions.pop(session_id, None)
                        logger.info(f"Session {session_id} zombie expired, deleted")
                    elif not sess.is_zombie:
                        logger.info(f"Session {session_id} revived, skipping deletion")

            asyncio.ensure_future(delayed_delete())
        else:
            logger.info(f"Peer left session {session_id} ({session.active_peers} remaining)")

    def cleanup_expired_sessions(self) -> int:
        expired = [sid for sid, session in self.sessions.items() if session.is_expired()]
        for sid in expired:
            self.sessions.pop(sid, None)
        if expired:
            logger.info(f"Cleaned up {len(expired)} expired sessions")
        return len(expired)


session_manager = SessionManager(session_ttl_minutes=60, zombie_delay_seconds=10)
