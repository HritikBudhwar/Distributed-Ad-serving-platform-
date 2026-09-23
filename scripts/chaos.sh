#!/usr/bin/env bash
# Chaos checks: process continues with degraded behavior. Run against a live compose stack.
set -euo pipefail
BASE=${BASE:-http://localhost:8000}

echo "== baseline =="
curl -s -X POST "$BASE/v1/ads/serve" -H 'content-type: application/json' \
  -d '{"query":"running shoes","geo":"IN"}' | python3 -m json.tool | head

echo "== stop redis (expect cache_hit=false, ads still returned) =="
docker compose stop redis || true
sleep 1
curl -s -X POST "$BASE/v1/ads/serve" -H 'content-type: application/json' \
  -d '{"query":"running shoes","geo":"IN"}' | python3 -m json.tool | head
docker compose start redis || true

echo "== stop indexer (expect failure_reason indexer_unavailable or no ads) =="
docker compose stop indexer || true
sleep 1
curl -s -X POST "$BASE/v1/ads/serve" -H 'content-type: application/json' \
  -d '{"query":"brand new query xyz","geo":"IN"}' | python3 -m json.tool | head
docker compose start indexer || true

echo "== stop kafka (expect ads still returned; indexing/impressions pause) =="
docker compose stop kafka || true
sleep 2
curl -s -X POST "$BASE/v1/ads/serve" -H 'content-type: application/json' \
  -d '{"query":"running shoes","geo":"IN"}' | python3 -m json.tool | head
docker compose start kafka || true

echo "Done. Inspect request_traces via analytics /v1/traces/{request_id}."
