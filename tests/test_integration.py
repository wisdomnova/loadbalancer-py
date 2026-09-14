import unittest
import asyncio
import json
from typing import Dict, Tuple, Optional
from loadbalancer.config import LoadBalancerConfig, BackendConfig, HealthCheckConfig
from loadbalancer.server import LoadBalancerServer


async def async_http_request(
    host: str,
    port: int,
    path: str,
    headers: Optional[Dict[str, str]] = None,
) -> Tuple[int, Dict[str, str], str]:
    """Async helper to make an HTTP GET request without blocking the event loop."""
    reader, writer = await asyncio.open_connection(host, port)
    req_lines = [f"GET {path} HTTP/1.1", f"Host: {host}:{port}", "Connection: close"]
    if headers:
        for k, v in headers.items():
            req_lines.append(f"{k}: {v}")
    req_lines.extend(["", ""])
    writer.write("\r\n".join(req_lines).encode())
    await writer.drain()

    status_line = await reader.readline()
    status_parts = status_line.decode().strip().split(" ", 2)
    status_code = int(status_parts[1]) if len(status_parts) >= 2 else 0

    resp_headers = {}
    while True:
        line = await reader.readline()
        if not line or line == b"\r\n":
            break
        decoded = line.decode().strip()
        if ":" in decoded:
            k, v = decoded.split(":", 1)
            # handle multiple Set-Cookie or general headers
            resp_headers[k.strip().lower()] = v.strip()

    body_bytes = await reader.read()
    writer.close()
    await writer.wait_closed()
    return status_code, resp_headers, body_bytes.decode()


class TestIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.backend_servers = []
        self.backend_counts = {"b1": 0, "b2": 0}
        self.b2_healthy = True

        async def make_handler(name: str):
            async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
                line = await reader.readline()
                if not line:
                    writer.close()
                    return
                parts = line.decode().split()
                path = parts[1] if len(parts) > 1 else "/"

                # Drain remaining headers
                while True:
                    h = await reader.readline()
                    if not h or h == b"\r\n":
                        break

                if path == "/health":
                    status = 200 if (name != "b2" or self.b2_healthy) else 500
                    resp = f"HTTP/1.1 {status} OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK"
                else:
                    self.backend_counts[name] += 1
                    body = json.dumps({"server": name, "count": self.backend_counts[name]})
                    resp = (
                        f"HTTP/1.1 200 OK\r\n"
                        f"Content-Type: application/json\r\n"
                        f"Content-Length: {len(body)}\r\n"
                        f"Connection: close\r\n\r\n{body}"
                    )
                writer.write(resp.encode())
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            return handler

        s1 = await asyncio.start_server(await make_handler("b1"), host="127.0.0.1", port=18001)
        s2 = await asyncio.start_server(await make_handler("b2"), host="127.0.0.1", port=18002)
        self.backend_servers.extend([s1, s2])

        # Start Load Balancer
        self.config = LoadBalancerConfig(
            host="127.0.0.1",
            port=18000,
            algorithm="round_robin",
            sticky_sessions=True,
            cookie_name="TEST_STICKY_COOKIE",
            backends=[
                BackendConfig(host="127.0.0.1", port=18001, server_id="b1"),
                BackendConfig(host="127.0.0.1", port=18002, server_id="b2"),
            ],
            health_check=HealthCheckConfig(
                enabled=True,
                interval_seconds=0.1,
                timeout_seconds=0.05,
                healthy_threshold=1,
                unhealthy_threshold=1,
            ),
        )
        self.lb = LoadBalancerServer(self.config)
        await self.lb.start()
        await asyncio.sleep(0.05)

    async def asyncTearDown(self):
        await self.lb.stop()
        for s in self.backend_servers:
            s.close()
            await s.wait_closed()

    async def test_round_robin_distribution(self):
        responses = []
        for _ in range(4):
            code, _, body = await async_http_request("127.0.0.1", 18000, "/api")
            self.assertEqual(code, 200)
            data = json.loads(body)
            responses.append(data["server"])

        self.assertEqual(responses, ["b1", "b2", "b1", "b2"])

    async def test_sticky_session_cookie(self):
        # 1st request gets Set-Cookie
        code, headers, body = await async_http_request("127.0.0.1", 18000, "/data")
        self.assertEqual(code, 200)
        data = json.loads(body)
        assigned_server = data["server"]
        set_cookie = headers.get("set-cookie", "")
        self.assertIn("TEST_STICKY_COOKIE=", set_cookie)

        # Extract cookie value
        cookie_header = set_cookie.split(";")[0]

        # Send 3 subsequent requests with this cookie
        for _ in range(3):
            code, _, body = await async_http_request(
                "127.0.0.1", 18000, "/data", headers={"Cookie": cookie_header}
            )
            self.assertEqual(code, 200)
            data = json.loads(body)
            self.assertEqual(data["server"], assigned_server)

    async def test_health_check_failover(self):
        # b2 becomes unhealthy
        self.b2_healthy = False
        await asyncio.sleep(0.25)

        # All requests must go to b1
        for _ in range(3):
            code, _, body = await async_http_request("127.0.0.1", 18000, "/ping")
            self.assertEqual(code, 200)
            data = json.loads(body)
            self.assertEqual(data["server"], "b1")

        # b2 recovers
        self.b2_healthy = True
        await asyncio.sleep(0.25)

        servers = set()
        for _ in range(4):
            code, _, body = await async_http_request("127.0.0.1", 18000, "/ping")
            self.assertEqual(code, 200)
            data = json.loads(body)
            servers.add(data["server"])

        self.assertIn("b1", servers)
        self.assertIn("b2", servers)

    async def test_lb_status_endpoint(self):
        code, _, body = await async_http_request("127.0.0.1", 18000, "/lb-status")
        self.assertEqual(code, 200)
        data = json.loads(body)
        self.assertIn("load_balancer", data)
        self.assertEqual(data["load_balancer"]["total_backends"], 2)
        self.assertEqual(data["load_balancer"]["healthy_backends"], 2)


if __name__ == "__main__":
    unittest.main()
