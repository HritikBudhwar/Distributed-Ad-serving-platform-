# Benchmarks

Fill this table from `python3 scripts/bench.py` and Locust. Do not invent numbers.

| Metric | Result | Command |
|---|---|---|
| Campaigns indexed | | `GET http://localhost:8002/health` (`index_size`) |
| Queries/sec | | `scripts/bench.py` |
| p50 latency | | `scripts/bench.py` |
| p95 latency | | `scripts/bench.py` |
| p99 latency | | `scripts/bench.py` |
| Cache hit rate | | Grafana / `adpulse_cache_hits_total` vs misses |
| Index update propagation | | time campaign PUT → indexer debug search |
| Concurrent requests | | Locust 100 / 500 / 1000 / 5000 users |
| Kafka throughput | | `kafka-exporter` / `kafka_topic_partition_current_offset` |
| Kafka consumer lag | | Grafana panel `kafka_consumergroup_lag` |
| Recovery time | | `scripts/chaos.sh` + wall clock to healthy |

Target for this project (goal, not a claimed result): **p99 < 100 ms** on a warm local compose stack for the seed catalog.
