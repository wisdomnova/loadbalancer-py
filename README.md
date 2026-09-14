# ⚡ PyLoadBalancer (`loadbalancer-py`)

A lightweight, high-performance **Layer-7 (HTTP) Load Balancer** built purely with Python's standard library `asyncio` (zero external dependencies).

Designed for clarity, extensibility, and practical real-world L7 features including active health monitoring, session stickiness, multiple balancing algorithms, and header injection.

---

## 🌟 Key Features

- 🚀 **Pure Python Standard Library**: Powered by `asyncio` with no external dependencies required.
- 🎯 **Layer-7 HTTP/1.1 Proxying**:
  - Full request and response header parsing.
  - Streaming payload forwarding (supports `Content-Length`, `Transfer-Encoding: chunked`, and EOF streaming).
  - Proxy header injection (`X-Forwarded-For`, `X-Forwarded-Proto`, `X-Forwarded-Host`, `X-Forwarded-Port`, `Via`).
- ⚖️ **Load Balancing Algorithms**:
  - **Round Robin**: Smooth cyclic distribution across healthy backends.
  - **Least Connections**: Dispatches traffic to the backend with the fewest active connections.
  - **IP Hash**: Deterministic hashing on client IP for consistent server affinity.
- 🍪 **Cookie-Based Session Stickiness**:
  - Automatically injects and tracks session cookies (`LB_SERVER_ID`).
  - Seamlessly re-routes to another healthy backend if a pinned backend goes down.
- 🩺 **Active Health Checking**:
  - Concurrent background task monitoring backend endpoints (e.g. `GET /health`).
  - Configurable healthy/unhealthy failure thresholds and polling intervals.
  - Automatic zero-downtime failover and recovery.
- 📊 **Real-time Observability & Metrics**:
  - Built-in `/lb-status` endpoint for monitoring backend states, active connections, total requests, and response latency.
  - Clean structured access logging with millisecond request timing.

---

## 📐 Architecture

```
                                  ┌───────────────────────────────┐
                                  │       PyLoadBalancer          │
                                  │   (asyncio.start_server)      │
                                  │                               │
                                  │  ┌─────────────────────────┐  │
[Client Request] ────────────────┼─►│  HTTP Parser & Dispatch  │  │
                                  │  │  - Parses Request/Head  │  │
                                  │  │  - Sticky Cookie Check  │  │
                                  │  │  - Header Rewriting     │  │
                                  │  │    (X-Forwarded-For...) │  │
                                  │  └────────────┬────────────┘  │
                                  │               │               │
                                  │  ┌────────────▼────────────┐  │
                                  │  │ Routing / Balancing     │  │
                                  │  │ - Round Robin           │  │
                                  │  │ - Least Connections     │  │
                                  │  │ - IP Hash               │  │
                                  │  └────────────┬────────────┘  │
                                  │               │               │
                                  │  ┌────────────▼────────────┐  │         ┌───────────────────┐
                                  │  │ Upstream Proxy Streamer ├──┼────────►│ Backend Server #1 │
                                  │  │ - Pipes Request & Resp  │  │         │ (127.0.0.1:8001)  │
                                  │  │ - Tracks Active Conns   │  │         └───────────────────┘
                                  │  │ - Injects Sticky Cookie │  │         ┌───────────────────┐
                                  │  └─────────────────────────┘  │ ───────►│ Backend Server #2 │
                                  │                               │         │ (127.0.0.1:8002)  │
                                  │  ┌─────────────────────────┐  │         └───────────────────┘
                                  │  │ Active Health Checker   ├──┼───────► ┌───────────────────┐
                                  │  │ (Async background task) │  │ ───────►│ Backend Server #3 │
                                  │  └─────────────────────────┘  │         │ (127.0.0.1:8003)  │
                                  │                               │         └───────────────────┘
                                  └───────────────────────────────┘
```

---

## 🚀 Quick Start

### 1. Start the Demo Mock Backends
Spin up 3 mock HTTP backend servers on ports `8001`, `8002`, and `8003`:

```bash
python3 demo_backends.py
```

### 2. Start the Load Balancer
In another terminal, launch the load balancer on port `8000`:

```bash
python3 main.py --config config.json
```

### 3. Send Requests
Send requests to the load balancer:

```bash
# Basic request
curl http://127.0.0.1:8000/

# Test cookie stickiness
curl -c cookies.txt http://127.0.0.1:8000/
curl -b cookies.txt http://127.0.0.1:8000/

# Check Load Balancer status & metrics
curl http://127.0.0.1:8000/lb-status
```

---

## ⚙️ Configuration (`config.json`)

```json
{
  "host": "127.0.0.1",
  "port": 8000,
  "algorithm": "round_robin",
  "sticky_sessions": true,
  "cookie_name": "LB_SERVER_ID",
  "cookie_max_age": 3600,
  "connection_timeout": 5.0,
  "backends": [
    {
      "host": "127.0.0.1",
      "port": 8001,
      "server_id": "backend-1",
      "weight": 1
    },
    {
      "host": "127.0.0.1",
      "port": 8002,
      "server_id": "backend-2",
      "weight": 1
    },
    {
      "host": "127.0.0.1",
      "port": 8003,
      "server_id": "backend-3",
      "weight": 1
    }
  ],
  "health_check": {
    "enabled": true,
    "interval_seconds": 3.0,
    "timeout_seconds": 1.5,
    "path": "/health",
    "healthy_threshold": 1,
    "unhealthy_threshold": 2
  }
}
```

---

## 🛠️ CLI Options

```bash
python3 main.py --help
```

| Flag | Type | Description |
| :--- | :--- | :--- |
| `-c, --config` | `str` | Path to JSON config file (default: `config.json`) |
| `-p, --port` | `int` | Override listening port (e.g. `8000`) |
| `-a, --algorithm` | `str` | Algorithm: `round_robin`, `least_connections`, `ip_hash` |
| `--sticky` | flag | Enable cookie session stickiness |
| `--no-sticky` | flag | Disable cookie session stickiness |
| `-v, --verbose` | flag | Enable debug-level logging |

---

## 🧪 Running Tests

Run the full unit and integration test suite:

```bash
python3 -m unittest discover tests/ -v
```

---

## 📦 Project Structure

```
loadbalancer-py/
├── loadbalancer/
│   ├── __init__.py         # Package exports
│   ├── config.py           # Dataclass configs & JSON schema loader
│   ├── backend.py          # BackendServer runtime state & metrics
│   ├── algorithms.py       # RoundRobin, LeastConnections, IPHash
│   ├── health_check.py     # Async background health checker
│   ├── http_parser.py      # HTTP request parsing & header rewriting
│   ├── proxy.py            # Stream forwarder, sticky sessions & error handler
│   └── server.py           # Asyncio TCP server lifecycle
├── demo_backends.py        # 3 mock HTTP backend servers for testing
├── main.py                 # CLI entry point
├── config.json             # Sample configuration
├── tests/
│   ├── test_algorithms.py  # Unit tests for algorithms
│   ├── test_http_parser.py # Unit tests for HTTP parsing & header modification
│   └── test_integration.py # End-to-end integration tests
├── .gitignore
└── README.md
```

---

## 📄 License
MIT
