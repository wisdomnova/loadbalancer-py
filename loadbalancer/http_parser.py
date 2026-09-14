import asyncio
from http.cookies import SimpleCookie
from typing import Dict, List, Optional, Tuple


class HTTPRequest:
    def __init__(
        self,
        method: str,
        path: str,
        version: str,
        headers: List[Tuple[str, str]],
        raw_headers: bytes,
    ):
        self.method = method
        self.path = path
        self.version = version
        self.headers = headers
        self.raw_headers = raw_headers
        self._header_map: Dict[str, str] = {}
        for k, v in headers:
            # lowercased key for case-insensitive lookup
            self._header_map[k.lower()] = v

    def get_header(self, name: str, default: Optional[str] = None) -> Optional[str]:
        return self._header_map.get(name.lower(), default)

    def get_cookies(self) -> Dict[str, str]:
        cookie_header = self.get_header("cookie")
        if not cookie_header:
            return {}
        cookie = SimpleCookie()
        try:
            cookie.load(cookie_header)
            return {k: v.value for k, v in cookie.items()}
        except Exception:
            return {}

    def get_content_length(self) -> int:
        cl = self.get_header("content-length")
        if cl and cl.isdigit():
            return int(cl)
        return 0

    def is_chunked(self) -> bool:
        te = self.get_header("transfer-encoding")
        return bool(te and "chunked" in te.lower())

    def build_forwarded_headers(
        self,
        client_ip: str,
        client_port: int,
        target_host: str,
        target_port: int,
        proto: str = "http",
    ) -> bytes:
        """Reconstructs request headers injecting X-Forwarded-* and Via headers."""
        new_headers: List[Tuple[str, str]] = []
        existing_xff = self.get_header("x-forwarded-for")
        
        # Determine updated XFF
        xff = f"{existing_xff}, {client_ip}" if existing_xff else client_ip

        # Filter and append modified headers
        skip_headers = {
            "x-forwarded-for",
            "x-forwarded-proto",
            "x-forwarded-host",
            "x-forwarded-port",
            "via",
        }

        for name, value in self.headers:
            if name.lower() not in skip_headers:
                new_headers.append((name, value))

        # Inject proxy headers
        new_headers.append(("X-Forwarded-For", xff))
        new_headers.append(("X-Forwarded-Proto", proto))
        new_headers.append(("X-Forwarded-Host", self.get_header("host") or f"{target_host}:{target_port}"))
        new_headers.append(("X-Forwarded-Port", str(client_port)))
        new_headers.append(("Via", "1.1 py-loadbalancer"))

        # Build payload
        lines = [f"{self.method} {self.path} {self.version}"]
        for name, val in new_headers:
            lines.append(f"{name}: {val}")
        lines.append("")
        lines.append("")
        return "\r\n".join(lines).encode("utf-8")


async def parse_http_request(reader: asyncio.StreamReader) -> Optional[HTTPRequest]:
    """Reads and parses an HTTP request line and headers from a StreamReader."""
    try:
        # Read request line
        request_line = await reader.readline()
        if not request_line:
            return None

        line_str = request_line.decode("utf-8", errors="replace").strip()
        parts = line_str.split()
        if len(parts) < 3:
            return None

        method, path, version = parts[0], parts[1], parts[2]

        # Read headers
        headers: List[Tuple[str, str]] = []
        raw_header_lines = [request_line]

        while True:
            header_line = await reader.readline()
            if not header_line:
                break
            raw_header_lines.append(header_line)
            decoded_header = header_line.decode("utf-8", errors="replace").strip()
            if not decoded_header:
                # Empty line marks end of headers
                break
            if ":" in decoded_header:
                k, v = decoded_header.split(":", 1)
                headers.append((k.strip(), v.strip()))

        return HTTPRequest(
            method=method,
            path=path,
            version=version,
            headers=headers,
            raw_headers=b"".join(raw_header_lines),
        )
    except Exception:
        return None
