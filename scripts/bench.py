#!/usr/bin/env python3
"""Measure serving latency. Prints a markdown table; does not invent numbers."""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time

import httpx


async def run(url: str, n: int, concurrency: int, query: str) -> None:
    latencies: list[float] = []
    errors = 0
    sem = asyncio.Semaphore(concurrency)

    async def one(client: httpx.AsyncClient) -> None:
        nonlocal errors
        async with sem:
            t0 = time.perf_counter()
            try:
                r = await client.post(
                    f"{url}/v1/ads/serve",
                    json={"query": query, "geo": "IN"},
                    timeout=5.0,
                )
                if r.status_code >= 400:
                    errors += 1
            except Exception:
                errors += 1
            latencies.append((time.perf_counter() - t0) * 1000)

    async with httpx.AsyncClient() as client:
        t0 = time.perf_counter()
        await asyncio.gather(*[one(client) for _ in range(n)])
        elapsed = time.perf_counter() - t0

    latencies.sort()
    def pct(p: float) -> float:
        if not latencies:
            return 0.0
        idx = min(int(p * (len(latencies) - 1)), len(latencies) - 1)
        return latencies[idx]

    qps = n / elapsed if elapsed else 0
    print("| Metric | Result |")
    print("|---|---|")
    print(f"| Requests | {n} |")
    print(f"| Concurrency | {concurrency} |")
    print(f"| Errors | {errors} |")
    print(f"| Queries/sec | {qps:.1f} |")
    print(f"| p50 latency | {pct(0.50):.2f} ms |")
    print(f"| p95 latency | {pct(0.95):.2f} ms |")
    print(f"| p99 latency | {pct(0.99):.2f} ms |")
    if latencies:
        print(f"| mean latency | {statistics.fmean(latencies):.2f} ms |")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://localhost:8000")
    p.add_argument("--n", type=int, default=200)
    p.add_argument("--concurrency", type=int, default=50)
    p.add_argument("--query", default="running shoes")
    args = p.parse_args()
    asyncio.run(run(args.url, args.n, args.concurrency, args.query))
