import asyncio
import time
from typing import Optional
from .config import BackendConfig


class BackendServer:
    """Represents a backend server instance and tracks its runtime state."""

    def __init__(self, config: BackendConfig):
        self.config = config
        self.host = config.host
        self.port = config.port
        self.server_id = config.server_id or f"{self.host}:{self.port}"
        self.weight = config.weight

        # Runtime state
        self.is_alive = True
        self.active_connections = 0
        self.total_requests = 0
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        self.last_checked_at: Optional[float] = None
        self.last_response_time_ms: Optional[float] = None

        # Lock for safe thread/async state updates
        self._lock = asyncio.Lock()

    def __repr__(self) -> str:
        state = "HEALTHY" if self.is_alive else "UNHEALTHY"
        return f"<BackendServer {self.server_id} ({state}) conns={self.active_connections} reqs={self.total_requests}>"

    async def increment_connections(self):
        async with self._lock:
            self.active_connections += 1
            self.total_requests += 1

    async def decrement_connections(self):
        async with self._lock:
            self.active_connections = max(0, self.active_connections - 1)

    async def record_health_check(self, success: bool, response_time_ms: float, healthy_thresh: int, unhealthy_thresh: int):
        async with self._lock:
            self.last_checked_at = time.time()
            self.last_response_time_ms = response_time_ms
            if success:
                self.consecutive_successes += 1
                self.consecutive_failures = 0
                if not self.is_alive and self.consecutive_successes >= healthy_thresh:
                    self.is_alive = True
            else:
                self.consecutive_failures += 1
                self.consecutive_successes = 0
                if self.is_alive and self.consecutive_failures >= unhealthy_thresh:
                    self.is_alive = False

    def to_dict(self) -> dict:
        return {
            "server_id": self.server_id,
            "host": self.host,
            "port": self.port,
            "is_alive": self.is_alive,
            "active_connections": self.active_connections,
            "total_requests": self.total_requests,
            "last_response_time_ms": round(self.last_response_time_ms, 2) if self.last_response_time_ms is not None else None,
        }
