import unittest
import asyncio
from loadbalancer.config import BackendConfig
from loadbalancer.backend import BackendServer
from loadbalancer.algorithms import (
    RoundRobinAlgorithm,
    LeastConnectionsAlgorithm,
    IPHashAlgorithm,
    get_algorithm,
)


class TestAlgorithms(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.b1 = BackendServer(BackendConfig(host="127.0.0.1", port=8001, server_id="b1"))
        self.b2 = BackendServer(BackendConfig(host="127.0.0.1", port=8002, server_id="b2"))
        self.b3 = BackendServer(BackendConfig(host="127.0.0.1", port=8003, server_id="b3"))
        self.backends = [self.b1, self.b2, self.b3]

    async def test_round_robin_cycling(self):
        rr = RoundRobinAlgorithm(self.backends)
        picks = [await rr.select_backend() for _ in range(6)]
        server_ids = [p.server_id for p in picks]
        self.assertEqual(server_ids, ["b1", "b2", "b3", "b1", "b2", "b3"])

    async def test_round_robin_skips_unhealthy(self):
        rr = RoundRobinAlgorithm(self.backends)
        self.b2.is_alive = False

        picks = [await rr.select_backend() for _ in range(4)]
        server_ids = [p.server_id for p in picks]
        self.assertEqual(server_ids, ["b1", "b3", "b1", "b3"])

    async def test_least_connections(self):
        lc = LeastConnectionsAlgorithm(self.backends)
        self.b1.active_connections = 5
        self.b2.active_connections = 1
        self.b3.active_connections = 3

        selected = await lc.select_backend()
        self.assertEqual(selected.server_id, "b2")

        # Increment b2 connections
        self.b2.active_connections = 4
        selected = await lc.select_backend()
        self.assertEqual(selected.server_id, "b3")

    async def test_ip_hash_consistency(self):
        ip_hash = IPHashAlgorithm(self.backends)
        client_a = "192.168.1.50"
        client_b = "10.0.0.22"

        pick_a1 = await ip_hash.select_backend(client_ip=client_a)
        pick_a2 = await ip_hash.select_backend(client_ip=client_a)
        self.assertEqual(pick_a1.server_id, pick_a2.server_id)

        pick_b1 = await ip_hash.select_backend(client_ip=client_b)
        pick_b2 = await ip_hash.select_backend(client_ip=client_b)
        self.assertEqual(pick_b1.server_id, pick_b2.server_id)

    async def test_factory_helper(self):
        algo = get_algorithm("round_robin", self.backends)
        self.assertIsInstance(algo, RoundRobinAlgorithm)

        algo = get_algorithm("least_connections", self.backends)
        self.assertIsInstance(algo, LeastConnectionsAlgorithm)

        algo = get_algorithm("ip_hash", self.backends)
        self.assertIsInstance(algo, IPHashAlgorithm)

        with self.assertRaises(ValueError):
            get_algorithm("non_existent", self.backends)


if __name__ == "__main__":
    unittest.main()
