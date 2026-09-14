import abc
import hashlib
import asyncio
from typing import List, Optional
from .backend import BackendServer


class BaseAlgorithm(abc.ABC):
    """Abstract base class for load balancing algorithms."""

    def __init__(self, backends: List[BackendServer]):
        self.backends = backends

    def get_healthy_backends(self) -> List[BackendServer]:
        return [b for b in self.backends if b.is_alive]

    @abc.abstractmethod
    async def select_backend(self, client_ip: Optional[str] = None) -> Optional[BackendServer]:
        """Selects a backend server for the incoming request."""
        pass


class RoundRobinAlgorithm(BaseAlgorithm):
    """Round-robin algorithm distributing requests cyclically among healthy backends."""

    def __init__(self, backends: List[BackendServer]):
        super().__init__(backends)
        self._index = 0
        self._lock = asyncio.Lock()

    async def select_backend(self, client_ip: Optional[str] = None) -> Optional[BackendServer]:
        healthy = self.get_healthy_backends()
        if not healthy:
            return None

        async with self._lock:
            # Pick next healthy server
            selected = healthy[self._index % len(healthy)]
            self._index = (self._index + 1) % len(healthy)
            return selected


class LeastConnectionsAlgorithm(BaseAlgorithm):
    """Routes traffic to the healthy backend with the lowest number of active connections."""

    async def select_backend(self, client_ip: Optional[str] = None) -> Optional[BackendServer]:
        healthy = self.get_healthy_backends()
        if not healthy:
            return None

        # Min active connections (with tie-breaker being least total requests)
        return min(healthy, key=lambda b: (b.active_connections, b.total_requests))


class IPHashAlgorithm(BaseAlgorithm):
    """Maps client IP deterministically to a healthy backend using consistent hashing."""

    async def select_backend(self, client_ip: Optional[str] = None) -> Optional[BackendServer]:
        healthy = self.get_healthy_backends()
        if not healthy:
            return None

        ip_key = (client_ip or "127.0.0.1").encode("utf-8")
        hash_val = int(hashlib.md5(ip_key).hexdigest(), 16)
        index = hash_val % len(healthy)
        return healthy[index]


def get_algorithm(name: str, backends: List[BackendServer]) -> BaseAlgorithm:
    algos = {
        "round_robin": RoundRobinAlgorithm,
        "least_connections": LeastConnectionsAlgorithm,
        "ip_hash": IPHashAlgorithm,
    }
    key = name.lower().strip().replace("-", "_")
    if key not in algos:
        raise ValueError(f"Unknown algorithm '{name}'. Available: {list(algos.keys())}")
    return algos[key](backends)
