import argparse
import asyncio
import logging
import sys
from loadbalancer.config import LoadBalancerConfig
from loadbalancer.server import LoadBalancerServer


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Lightweight Functional L7 HTTP Load Balancer in Python",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-c", "--config",
        type=str,
        default="config.json",
        help="Path to JSON configuration file",
    )
    parser.add_argument(
        "-p", "--port",
        type=int,
        default=None,
        help="Override listening port",
    )
    parser.add_argument(
        "-a", "--algorithm",
        type=str,
        choices=["round_robin", "least_connections", "ip_hash"],
        default=None,
        help="Override load balancing algorithm",
    )
    parser.add_argument(
        "--sticky",
        action="store_true",
        default=None,
        help="Enable sticky sessions (cookie-based)",
    )
    parser.add_argument(
        "--no-sticky",
        action="store_true",
        default=False,
        help="Disable sticky sessions",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug level logging",
    )
    return parser.parse_args()


async def main():
    args = parse_args()
    setup_logging(args.verbose)

    try:
        config = LoadBalancerConfig.from_file(args.config)
    except FileNotFoundError:
        logging.error("Configuration file '%s' not found.", args.config)
        sys.exit(1)
    except Exception as e:
        logging.error("Failed to parse config file: %s", e)
        sys.exit(1)

    # CLI Overrides
    if args.port:
        config.port = args.port
    if args.algorithm:
        config.algorithm = args.algorithm
    if args.sticky:
        config.sticky_sessions = True
    elif args.no_sticky:
        config.sticky_sessions = False

    server = LoadBalancerServer(config)
    try:
        await server.run_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
