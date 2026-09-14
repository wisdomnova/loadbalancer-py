import unittest
import asyncio
from loadbalancer.http_parser import HTTPRequest, parse_http_request


class TestHTTPParser(unittest.IsolatedAsyncioTestCase):
    async def test_parse_simple_get(self):
        raw = (
            b"GET /api/users?page=1 HTTP/1.1\r\n"
            b"Host: example.com\r\n"
            b"User-Agent: curl/7.88.1\r\n"
            b"Accept: */*\r\n\r\n"
        )
        reader = asyncio.StreamReader()
        reader.feed_data(raw)
        reader.feed_eof()

        req = await parse_http_request(reader)
        self.assertIsNotNone(req)
        self.assertEqual(req.method, "GET")
        self.assertEqual(req.path, "/api/users?page=1")
        self.assertEqual(req.version, "HTTP/1.1")
        self.assertEqual(req.get_header("Host"), "example.com")
        self.assertEqual(req.get_header("host"), "example.com")  # case-insensitivity
        self.assertEqual(req.get_header("User-Agent"), "curl/7.88.1")

    async def test_cookie_extraction(self):
        raw = (
            b"GET /dashboard HTTP/1.1\r\n"
            b"Host: example.com\r\n"
            b"Cookie: session_id=xyz123; LB_SERVER_ID=backend-2; theme=dark\r\n\r\n"
        )
        reader = asyncio.StreamReader()
        reader.feed_data(raw)
        reader.feed_eof()

        req = await parse_http_request(reader)
        cookies = req.get_cookies()
        self.assertEqual(cookies.get("LB_SERVER_ID"), "backend-2")
        self.assertEqual(cookies.get("session_id"), "xyz123")
        self.assertEqual(cookies.get("theme"), "dark")

    async def test_forwarded_headers_injection(self):
        raw = (
            b"POST /submit HTTP/1.1\r\n"
            b"Host: myapp.com\r\n"
            b"Content-Length: 15\r\n\r\n"
        )
        reader = asyncio.StreamReader()
        reader.feed_data(raw)
        reader.feed_eof()

        req = await parse_http_request(reader)
        forwarded_bytes = req.build_forwarded_headers(
            client_ip="203.0.113.195",
            client_port=54321,
            target_host="10.0.0.5",
            target_port=8080,
            proto="http",
        )
        forwarded_text = forwarded_bytes.decode("utf-8")
        self.assertIn("X-Forwarded-For: 203.0.113.195", forwarded_text)
        self.assertIn("X-Forwarded-Proto: http", forwarded_text)
        self.assertIn("X-Forwarded-Port: 54321", forwarded_text)
        self.assertIn("Via: 1.1 py-loadbalancer", forwarded_text)
        self.assertIn("Host: myapp.com", forwarded_text)


if __name__ == "__main__":
    unittest.main()
