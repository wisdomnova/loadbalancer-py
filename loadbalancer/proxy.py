import asyncio
import json
import logging
import time
from typing import Dict, List, Optional, Tuple
from .config import LoadBalancerConfig
from .backend import BackendServer
from .algorithms import BaseAlgorithm
from .http_parser import HTTPRequest, parse_http_request

logger = logging.getLogger("loadbalancer.proxy")

CHUNK_SIZE = 64 * 1024  # 64 KB streaming buffer


class HTTPProxyHandler:
    def __init__(
        self,
        config: LoadBalancerConfig,
        backends: List[BackendServer],
        algorithm: BaseAlgorithm,
    ):
        self.config = config
        self.backends = backends
        self.backend_map: Dict[str, BackendServer] = {b.server_id: b for b in backends}
        self.algorithm = algorithm

    def _resolve_sticky_backend(self, request: HTTPRequest) -> Optional[BackendServer]:
        """Resolves backend from sticky cookie if present and healthy."""
        if not self.config.sticky_sessions:
            return None
        cookies = request.get_cookies()
        server_id = cookies.get(self.config.cookie_name)
        if server_id and server_id in self.backend_map:
            candidate = self.backend_map[server_id]
            if candidate.is_alive:
                return candidate
        return None

    async def handle_client(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ):
        peer = client_writer.get_extra_info("peername")
        client_ip = peer[0] if peer else "127.0.0.1"
        client_port = peer[1] if peer else 0

        start_time = time.perf_counter()

        try:
            # Parse incoming HTTP request
            request = await parse_http_request(client_reader)
            if not request:
                client_writer.close()
                await client_writer.wait_closed()
                return

            # Built-in stats endpoint
            if request.path == "/lb-status":
                await self._send_status_response(client_writer)
                return

            # Backend selection: Sticky Session vs Load Balancing Algorithm
            backend = self._resolve_sticky_backend(request)
            is_sticky_hit = backend is not None

            if not backend:
                backend = await self.algorithm.select_backend(client_ip=client_ip)

            if not backend:
                logger.error("No healthy backends available to handle request %s %s", request.method, request.path)
                await self._send_error_response(
                    client_writer,
                    status_code=503,
                    status_text="Service Unavailable",
                    message="503 Service Unavailable - No healthy backend servers available.",
                )
                return

            # Forward request to chosen backend
            await self._proxy_request(
                client_reader=client_reader,
                client_writer=client_writer,
                request=request,
                backend=backend,
                client_ip=client_ip,
                client_port=client_port,
                is_sticky_hit=is_sticky_hit,
                start_time=start_time,
            )

        except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            logger.error("Unexpected error handling client connection: %s", e, exc_info=True)
            try:
                await self._send_error_response(
                    client_writer,
                    status_code=502,
                    status_text="Bad Gateway",
                    message=f"502 Bad Gateway - Error contacting backend: {e}",
                )
            except Exception:
                pass
        finally:
            try:
                client_writer.close()
                await client_writer.wait_closed()
            except Exception:
                pass

    async def _proxy_request(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
        request: HTTPRequest,
        backend: BackendServer,
        client_ip: str,
        client_port: int,
        is_sticky_hit: bool,
        start_time: float,
    ):
        await backend.increment_connections()
        backend_reader = None
        backend_writer = None

        try:
            # Connect to backend
            backend_reader, backend_writer = await asyncio.wait_for(
                asyncio.open_connection(backend.host, backend.port),
                timeout=self.config.connection_timeout,
            )

            # Build modified request headers (injecting X-Forwarded-*, Via)
            forwarded_headers = request.build_forwarded_headers(
                client_ip=client_ip,
                client_port=client_port,
                target_host=backend.host,
                target_port=backend.port,
            )
            backend_writer.write(forwarded_headers)
            await backend_writer.drain()

            # Stream request body if present (e.g. POST/PUT payload)
            content_length = request.get_content_length()
            if content_length > 0:
                bytes_left = content_length
                while bytes_left > 0:
                    read_len = min(CHUNK_SIZE, bytes_left)
                    chunk = await client_reader.read(read_len)
                    if not chunk:
                        break
                    backend_writer.write(chunk)
                    bytes_left -= len(chunk)
                await backend_writer.drain()
            elif request.is_chunked():
                # Read chunks until 0\r\n\r\n
                while True:
                    line = await client_reader.readline()
                    backend_writer.write(line)
                    if not line:
                        break
                    size_str = line.strip().split(b";")[0]
                    chunk_size = int(size_str, 16) if size_str else 0
                    if chunk_size == 0:
                        # Final trailer line
                        trailer = await client_reader.readline()
                        backend_writer.write(trailer)
                        await backend_writer.drain()
                        break
                    chunk_data = await client_reader.readexactly(chunk_size + 2)  # data + \r\n
                    backend_writer.write(chunk_data)
                    await backend_writer.drain()

            # Read response from backend
            status_code, resp_headers = await self._read_backend_headers(backend_reader)

            # Inject sticky session cookie into response headers if enabled
            if self.config.sticky_sessions and not is_sticky_hit:
                cookie_val = (
                    f"{self.config.cookie_name}={backend.server_id}; "
                    f"Path=/; Max-Age={self.config.cookie_max_age}; HttpOnly; SameSite=Lax"
                )
                resp_headers.append(("Set-Cookie", cookie_val))

            # Send response headers to client
            resp_header_bytes = self._format_response_headers(resp_headers)
            client_writer.write(resp_header_bytes)
            await client_writer.drain()

            # Stream response body to client
            resp_content_length = self._get_header_val(resp_headers, "content-length")
            is_resp_chunked = "chunked" in (self._get_header_val(resp_headers, "transfer-encoding") or "").lower()

            if resp_content_length is not None and resp_content_length.isdigit():
                remaining = int(resp_content_length)
                while remaining > 0:
                    read_size = min(CHUNK_SIZE, remaining)
                    chunk = await backend_reader.read(read_size)
                    if not chunk:
                        break
                    client_writer.write(chunk)
                    remaining -= len(chunk)
                await client_writer.drain()
            elif is_resp_chunked:
                while True:
                    line = await backend_reader.readline()
                    client_writer.write(line)
                    if not line:
                        break
                    size_str = line.strip().split(b";")[0]
                    try:
                        chunk_size = int(size_str, 16) if size_str else 0
                    except ValueError:
                        chunk_size = 0
                    if chunk_size == 0:
                        trailer = await backend_reader.readline()
                        client_writer.write(trailer)
                        await client_writer.drain()
                        break
                    chunk_data = await backend_reader.readexactly(chunk_size + 2)
                    client_writer.write(chunk_data)
                    await client_writer.drain()
            else:
                # Stream until EOF (connection close)
                while True:
                    chunk = await backend_reader.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    client_writer.write(chunk)
                await client_writer.drain()

            elapsed_ms = (time.perf_counter() - start_time) * 1000.0
            sticky_flag = " (STICKY)" if is_sticky_hit else ""
            logger.info(
                "%s %s -> %s%s [Status: %s] in %.2fms",
                request.method,
                request.path,
                backend.server_id,
                sticky_flag,
                status_code,
                elapsed_ms,
            )

        finally:
            await backend.decrement_connections()
            if backend_writer:
                try:
                    backend_writer.close()
                    await backend_writer.wait_closed()
                except Exception:
                    pass

    async def _read_backend_headers(
        self, reader: asyncio.StreamReader
    ) -> Tuple[str, List[Tuple[str, str]]]:
        """Reads status line and headers from backend response."""
        status_line = await reader.readline()
        status_line_str = status_line.decode("utf-8", errors="replace").strip()
        parts = status_line_str.split(" ", 2)
        status_code = parts[1] if len(parts) >= 2 else "200"

        headers: List[Tuple[str, str]] = [("Status-Line", status_line_str)]
        while True:
            line = await reader.readline()
            if not line:
                break
            line_str = line.decode("utf-8", errors="replace").strip()
            if not line_str:
                break
            if ":" in line_str:
                k, v = line_str.split(":", 1)
                headers.append((k.strip(), v.strip()))

        return status_code, headers

    def _format_response_headers(self, headers: List[Tuple[str, str]]) -> bytes:
        status_line = "HTTP/1.1 200 OK"
        header_lines = []
        for k, v in headers:
            if k == "Status-Line":
                status_line = v
            else:
                header_lines.append(f"{k}: {v}")
        
        # Add load balancer header
        header_lines.append("X-Proxy-By: Python-L7-LoadBalancer")

        output = [status_line] + header_lines + ["", ""]
        return "\r\n".join(output).encode("utf-8")

    def _get_header_val(self, headers: List[Tuple[str, str]], name: str) -> Optional[str]:
        for k, v in headers:
            if k.lower() == name.lower():
                return v
        return None

    async def _send_status_response(self, writer: asyncio.StreamWriter):
        """Returns JSON metrics regarding load balancer state and backends."""
        status_payload = {
            "load_balancer": {
                "algorithm": self.config.algorithm,
                "sticky_sessions": self.config.sticky_sessions,
                "total_backends": len(self.backends),
                "healthy_backends": len([b for b in self.backends if b.is_alive]),
            },
            "backends": [b.to_dict() for b in self.backends],
        }
        body = json.dumps(status_payload, indent=2).encode("utf-8")
        headers = (
            f"HTTP/1.1 200 OK\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n\r\n"
        )
        writer.write(headers.encode("utf-8") + body)
        await writer.drain()

    async def _send_error_response(
        self,
        writer: asyncio.StreamWriter,
        status_code: int,
        status_text: str,
        message: str,
    ):
        body = (
            f"<!DOCTYPE html><html><head><title>{status_code} {status_text}</title></head>"
            f"<body><h1>{status_code} {status_text}</h1><p>{message}</p>"
            f"<hr><em>Python L7 Load Balancer</em></body></html>"
        ).encode("utf-8")
        resp = (
            f"HTTP/1.1 {status_code} {status_text}\r\n"
            f"Content-Type: text/html\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n\r\n"
        )
        writer.write(resp.encode("utf-8") + body)
        await writer.drain()
