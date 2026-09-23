# AdPulse

Distributed, low-latency ad retrieval and serving: **campaign ingestion → streaming index → retrieve → eligibility → rank → mini-auction → privacy gate → response**, with Kafka events, Redis caching, replay traces, and Prometheus metrics.

This is a production-style learning system aligned with search ads engineering (indexing, real-time delivery, streaming, ranking/auction, distributed services). It does **not** claim to reproduce any proprietary auction.

MiniRAFT stays a separate C++ project.

## Services (5)

| Service | Role | API |
|---|---|---|
| **Campaign** | Advertisers, campaigns, budgets, bids, targeting. Transactional write + outbox | REST `:8001` |
| **Indexer** | Kafka consumer, in-memory inverted index, Redis campaign cache | gRPC `:50051`, HTTP debug `:8002` |
| **Serving** (Rust) | Query processor, eligibility, ranking, auction, traces, impressions | REST `:8000` |
| **Analytics** | Impression consumer, daily rollups, trace lookup | REST `:8003` |
| **Privacy** | k-anonymity on cohort counts (no individual targeting) | gRPC `:50052`, HTTP `:8004` |

Infrastructure (not extra product services): **PostgreSQL**, **Redis**, and **Apache Kafka 3.7** (`apache/kafka` image, KRaft). Clients use the Kafka protocol (`aiokafka`, `rdkafka`). Hosts connect at `localhost:19092`; containers use `kafka:9092`. Topics `campaign.events` and `ad.impressions` are created with 3 partitions. Kafka lag is scraped via `kafka-exporter`.

```
Advertiser → Campaign REST → PostgreSQL → outbox → Kafka
                                              ↓
                                       Streaming Indexer
                                              ↓
User query → Serving (Rust) → Indexer gRPC → eligibility → rank → auction → Privacy gRPC → Ad JSON
                                              ↓
                                    Kafka impressions → Analytics → PostgreSQL
```

## Quick start

```bash
docker compose up --build
```

Wait until `serving` is healthy, then:

```bash
curl -s http://localhost:8000/health

curl -s -X POST http://localhost:8000/v1/ads/serve \
  -H 'content-type: application/json' \
  -d '{"query":"running shoes","geo":"IN","cohort_id":"cohort_runners_in"}'
```

Seed campaigns A/B/C match the running-shoes example in [docs/auction.md](docs/auction.md).

Create/update a campaign (streams into the index, no full rebuild):

```bash
curl -s -X POST http://localhost:8001/v1/campaigns \
  -H 'content-type: application/json' \
  -d '{
    "advertiser_id":"11111111-1111-1111-1111-111111111111",
    "name":"New trail",
    "headline":"Trail running shoes",
    "landing_url":"https://example.com/new",
    "keywords":["running","shoes","trail"],
    "bid_micros":4500000,
    "daily_budget_micros":100000000,
    "status":"active",
    "geo_targets":["IN"]
  }'
```

Replay a request (use `request_id` from the serve response):

```bash
curl -s http://localhost:8003/v1/traces/<request_id>
```

Grafana: http://localhost:3000 (`admin` / `admin`). Prometheus: http://localhost:9090.

## Auction

`score = relevance × bid`. Highest score wins; price is a second-price transform back into bid units. Details and the A/B/C numeric example: [docs/auction.md](docs/auction.md).

## Privacy

Cohorts are identifiers you already hashed (`cohort_id`). Targeting using a cohort requires `member_count >= k` (default 50). Below `k`, serving still returns **contextual** ads from the query, not individual-level targeting. Tiny seed cohort `cohort_tiny` (12 members) is blocked for cohort use.

## Failure handling and idempotency

See [docs/failure_handling.md](docs/failure_handling.md). Duplicate Kafka `event_id`s are ignored via `processed_events` and the in-memory index.

## Postgres query optimization

Indexes and a before/after EXPLAIN workflow: [docs/query_optimization.md](docs/query_optimization.md). Run `make explain` on a live database and paste **measured** times; do not fabricate them.

## Load and latency

```bash
python3 -m pip install httpx
python3 scripts/bench.py --n 200 --concurrency 50
python3 scripts/bench.py --n 1000 --concurrency 500

# Locust UI
pip install locust
locust -f loadtest/locustfile.py --host http://localhost:8000
```

Record results in [docs/benchmarks.md](docs/benchmarks.md). Goal on a warm local stack: **p99 < 100 ms** for the seed catalog.

Chaos (Redis/indexer down): `bash scripts/chaos.sh`

## Kubernetes

Images are `adpulse/<service>:local` (build with compose or `docker build`). Apply:

```bash
kubectl apply -k k8s
```

Serving has an HPA (CPU 70%, 2–8 replicas). Infra manifests are for demos; use managed Postgres/Redis/Kafka in a real environment.

## Tests / CI

```bash
pip install pytest ./packages/common
PYTHONPATH=services/indexer pytest packages/common services/indexer/test_index.py services/privacy/test_privacy.py
cd services/serving && cargo test
```

GitHub Actions runs the same suite.

## Tech stack

| Layer | Technology | In this repo |
|---|---|---|
| Systems / ad server | Rust | `services/serving` |
| Existing systems project | C++ | MiniRAFT — separate repo |
| Campaign API | Python + FastAPI | `services/campaign` |
| Internal communication | gRPC | indexer + privacy; Rust tonic client |
| Primary DB | PostgreSQL | schema in `sql/` |
| Cache / KV | Redis | query + campaign cache |
| Streaming | Apache Kafka 3.7 | `campaign.events`, `ad.impressions` |
| Search | Custom inverted index | `services/indexer` |
| Ranking | Rust | relevance × bid, second-price |
| Analytics | Python | `services/analytics` |
| Containers | Docker Compose | `docker-compose.yml` |
| Orchestration | Kubernetes | `k8s/` |
| Monitoring | Prometheus + Grafana | QPS, latency, errors, Kafka lag |
| CI/CD | GitHub Actions | `.github/workflows/ci.yml` |
| Testing | Rust tests + Pytest | `cargo test`, `pytest` |
| Load testing | Locust / `scripts/bench.py` | concurrent latency |
| Version control | Git + GitHub | this repository |

## Layout

```
proto/                 gRPC (indexer + privacy)
sql/                   schema, indexes, seed
services/campaign      FastAPI REST + outbox publisher
services/indexer       inverted index + Kafka + gRPC
services/serving       Rust axum + tonic + auction
services/analytics     Kafka → Postgres aggregates
services/privacy       k-threshold gRPC
k8s/                   Deployments, services, HPA
observability/         Prometheus + Grafana dashboard
loadtest/              Locust
```
