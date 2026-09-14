import asyncio
import logging
import time
from typing import List
from .backend import BackendServer
from .config import HealthCheckConfig

logger = logging.getLogger("loadbalancer.health_check")


class HealthChecker:
    """Periodically probes backend servers to monitor their health status."""

    def __init__(self, backends: List[BackendServer], config: HealthCheckConfig):
        self.backends = backends
        self.config = config
        self._running = False
        self._task: asyncio.Task | None = None

    async def check_backend(self, backend: BackendServer) -> bool:
        """Sends an HTTP GET request to backend's health check path."""
        start_time = time.perf_counter()
        success = False
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(backend.host, backend.port),
                timeout=self.config.timeout_seconds,
            )
            # Send minimal HTTP request
            request = (
                f"GET {self.config.path} HTTP/1.1\r\n"
                f"Host: {backend.host}:{backend.port}\r\n"
                f"User-Agent: PyLoadBalancer-HealthChecker\r\n"
                f"Connection: close\r\n\r\n"
            )
            writer.write(request.encode("utf-8"))
            await writer.drain()

            # Read response line
            response_line = await asyncio.wait_for(
                reader.readline(),
                timeout=self.config.timeout_seconds,
            )
            writer.close()
            await writer.wait_closed()

            # Expect HTTP/1.x 200..399
            line_str = response_line.decode("utf-8", errors="ignore").strip()
            parts = line_str.split(" ")
            if len(parts) >= 2 and parts[1].isdigit():
                status_code = int(parts[1])
                success = 200 <= status_code < 400
            else:
                success = False

        except Exception as e:
            logger.debug("Health check failed for %s:%s - %s", backend.host, backend.port, e)
            success = False

        elapsed_ms = (time.perf_counter() - start_time) * 1000.0
        prev_status = backend.is_alive
        await backend.record_health_check(
            success=success,
            response_time_ms=elapsed_ms,
            healthy_thresh=self.config.healthy_threshold,
            unhealthy_thresh=self.config.unhealthy_threshold,
        )

        if prev_status != backend.is_alive:
            state = "UP" if backend.is_alive else "DOWN"
            logger.warning(
                "Backend %s transitioned to %s (latency: %.1fms)",
                backend.server_id,
                state,
                elapsed_ms,
            )

        return success

    async def _check_all(self):
        """Runs health checks concurrently across all backends."""
        tasks = [self.check_backend(b) for b in self.backends]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_loop(self):
        logger.info(
            "Health checker started (interval: %.1fs, path: %s)",
            self.config.interval_seconds,
            self.config.path,
        )
        # Initial health check run immediately
        await self._check_all()

        while self._running:
            try:
                await asyncio.sleep(self.config.interval_seconds)
                if not self._running:
                    break
                await self._check_all()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in health check loop: %s", e)

    def start(self):
        if not self.config.enabled:
            logger.info("Health checks are disabled in config.")
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Health checker stopped.")
