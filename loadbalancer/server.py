import asyncio
import logging
import signal
from typing import List, Optional
from .config import LoadBalancerConfig
from .backend import BackendServer
from .algorithms import get_algorithm, BaseAlgorithm
from .health_check import HealthChecker
from .proxy import HTTPProxyHandler

logger = logging.getLogger("loadbalancer.server")


class LoadBalancerServer:
    def __init__(self, config: LoadBalancerConfig):
        self.config = config
        self.backends: List[BackendServer] = [BackendServer(b) for b in config.backends]
        self.algorithm: BaseAlgorithm = get_algorithm(config.algorithm, self.backends)
        self.health_checker = HealthChecker(self.backends, config.health_check)
        self.proxy_handler = HTTPProxyHandler(config, self.backends, self.algorithm)

        self._server: Optional[asyncio.Server] = None
        self._shutdown_event = asyncio.Event()

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        await self.proxy_handler.handle_client(reader, writer)

    async def start(self):
        """Starts the load balancer server and background health checker."""
        logger.info(
            "Starting Load Balancer on %s:%s using algorithm '%s' (Sticky Sessions: %s)",
            self.config.host,
            self.config.port,
            self.config.algorithm,
            self.config.sticky_sessions,
        )
        logger.info("Registered %d backend servers:", len(self.backends))
        for b in self.backends:
            logger.info("  - %s (%s:%s)", b.server_id, b.host, b.port)

        # Start health check daemon
        self.health_checker.start()

        # Start TCP server
        self._server = await asyncio.start_server(
            self._handle_connection,
            host=self.config.host,
            port=self.config.port,
            reuse_port=True if hasattr(asyncio, "SO_REUSEPORT") else False,
        )

        addrs = ", ".join(str(sock.getsockname()) for sock in self._server.sockets)
        logger.info("Serving HTTP traffic on %s", addrs)

    async def stop(self):
        """Stops the load balancer gracefully."""
        logger.info("Initiating graceful shutdown...")
        await self.health_checker.stop()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        logger.info("Load balancer stopped cleanly.")

    async def run_forever(self):
        """Runs the server until a termination signal is received."""
        await self.start()
        loop = asyncio.get_running_loop()

        # Attach signal handlers if supported
        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self._shutdown_event.set)
        except (NotImplementedError, RuntimeError):
            pass

        try:
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()
