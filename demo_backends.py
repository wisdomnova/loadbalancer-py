"""
Mock Backend Servers for Testing and Demonstrating the Load Balancer.
Spins up 3 lightweight HTTP servers concurrently on ports 8001, 8002, and 8003.
"""

import asyncio
import json
import logging
import sys
from urllib.parse import parse_qs, urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
)
logger = logging.getLogger("demo_backends")


class MockBackend:
    def __init__(self, name: str, port: int):
        self.name = name
        self.port = port
        self.is_healthy = True
        self.request_count = 0
        self.server: asyncio.Server | None = None

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.request_count += 1
        try:
            request_line = await reader.readline()
            if not request_line:
                writer.close()
                return

            line_str = request_line.decode("utf-8", errors="ignore").strip()
            parts = line_str.split(" ")
            if len(parts) < 2:
                writer.close()
                return

            method, raw_path = parts[0], parts[1]
            parsed_url = urlparse(raw_path)
            path = parsed_url.path
            query_params = parse_qs(parsed_url.query)

            headers = {}
            while True:
                header_line = await reader.readline()
                if not header_line:
                    break
                decoded = header_line.decode("utf-8", errors="ignore").strip()
                if not decoded:
                    break
                if ":" in decoded:
                    k, v = decoded.split(":", 1)
                    headers[k.strip()] = v.strip()

            # Read body if present
            body_bytes = b""
            if "content-length" in {k.lower(): v for k, v in headers.items()}:
                cl = int([v for k, v in headers.items() if k.lower() == "content-length"][0])
                body_bytes = await reader.read(cl)

            # Route handling
            if path == "/health":
                if self.is_healthy:
                    self._send_json(writer, 200, {"status": "healthy", "server": self.name, "port": self.port})
                else:
                    self._send_json(writer, 500, {"status": "unhealthy", "server": self.name, "port": self.port})
            elif path == "/toggle-health":
                self.is_healthy = not self.is_healthy
                status = "healthy" if self.is_healthy else "unhealthy"
                self._send_json(writer, 200, {"message": f"{self.name} is now {status}", "healthy": self.is_healthy})
            elif path == "/slow":
                delay_ms = float(query_params.get("ms", [500])[0])
                await asyncio.sleep(delay_ms / 1000.0)
                self._send_json(writer, 200, {
                    "server": self.name,
                    "port": self.port,
                    "delayed_ms": delay_ms,
                    "request_count": self.request_count,
                })
            else:
                # Default response returning server info and received headers
                response_data = {
                    "message": f"Hello from {self.name}!",
                    "server_id": self.name,
                    "port": self.port,
                    "requests_served": self.request_count,
                    "path": path,
                    "method": method,
                    "received_headers": headers,
                    "received_body": body_bytes.decode("utf-8", errors="ignore") if body_bytes else None,
                }
                self._send_json(writer, 200, response_data)

        except Exception as e:
            logger.error("[%s] Error: %s", self.name, e)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    def _send_json(self, writer: asyncio.StreamWriter, status_code: int, data: dict):
        status_text = "OK" if status_code == 200 else ("Internal Server Error" if status_code == 500 else "Error")
        body = json.dumps(data, indent=2).encode("utf-8")
        resp = (
            f"HTTP/1.1 {status_code} {status_text}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode("utf-8") + body
        writer.write(resp)

    async def start(self):
        self.server = await asyncio.start_server(self.handle_client, host="127.0.0.1", port=self.port)
        logger.info("Mock Backend '%s' listening on http://127.0.0.1:%d", self.name, self.port)

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()


async def run_demo_backends():
    backends = [
        MockBackend("backend-1", 8001),
        MockBackend("backend-2", 8002),
        MockBackend("backend-3", 8003),
    ]
    for b in backends:
        await b.start()

    logger.info("All 3 mock backends running. Press Ctrl+C to stop.")
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Stopping backends...")
        for b in backends:
            await b.stop()


if __name__ == "__main__":
    try:
        asyncio.run(run_demo_backends())
    except KeyboardInterrupt:
        pass
