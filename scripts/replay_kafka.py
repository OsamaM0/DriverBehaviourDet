"""Replay a Kafka topic into another (e.g. for replaying frames in a backtest).

Usage: python -m scripts.replay_kafka --src events.alert --dst events.alert.replay
"""
import argparse
import asyncio

from packages.common.kafka import bus, iter_topic


async def run(src: str, dst: str) -> None:
    async with bus.producer() as prod:
        async for raw in iter_topic([src], group_id=f"replay-{src}"):
            await prod.send_and_wait(dst, raw)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--src", required=True)
    p.add_argument("--dst", required=True)
    args = p.parse_args()
    asyncio.run(run(args.src, args.dst))


if __name__ == "__main__":
    main()
