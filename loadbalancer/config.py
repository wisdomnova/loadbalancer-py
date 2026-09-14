from dataclasses import dataclass, field
from typing import List, Optional
import json
import os


@dataclass
class BackendConfig:
    host: str
    port: int
    weight: int = 1
    server_id: Optional[str] = None

    def __post_init__(self):
        if not self.server_id:
            self.server_id = f"{self.host}:{self.port}"


@dataclass
class HealthCheckConfig:
    enabled: bool = True
    interval_seconds: float = 5.0
    timeout_seconds: float = 2.0
    path: str = "/health"
    unhealthy_threshold: int = 2
    healthy_threshold: int = 2


@dataclass
class LoadBalancerConfig:
    host: str = "0.0.0.0"
    port: int = 8000
    algorithm: str = "round_robin"  # round_robin | least_connections | ip_hash
    sticky_sessions: bool = False
    cookie_name: str = "LB_SERVER_ID"
    cookie_max_age: int = 3600
    backends: List[BackendConfig] = field(default_factory=list)
    health_check: HealthCheckConfig = field(default_factory=HealthCheckConfig)
    connection_timeout: float = 10.0

    @classmethod
    def from_dict(cls, data: dict) -> "LoadBalancerConfig":
        backends_data = data.get("backends", [])
        backends = [BackendConfig(**b) for b in backends_data]
        
        health_data = data.get("health_check", {})
        health_check = HealthCheckConfig(**health_data)

        return cls(
            host=data.get("host", "0.0.0.0"),
            port=data.get("port", 8000),
            algorithm=data.get("algorithm", "round_robin"),
            sticky_sessions=data.get("sticky_sessions", False),
            cookie_name=data.get("cookie_name", "LB_SERVER_ID"),
            cookie_max_age=data.get("cookie_max_age", 3600),
            backends=backends,
            health_check=health_check,
            connection_timeout=data.get("connection_timeout", 10.0),
        )

    @classmethod
    def from_file(cls, path: str) -> "LoadBalancerConfig":
        if not os.path.exists(path):
            raise FileNotFoundError(f"Configuration file not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls.from_dict(data)
