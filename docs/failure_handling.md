# Failure handling

Serving is designed to return a response even when dependencies fail.

| Dependency | Failure | Serving behavior |
|---|---|---|
| Redis | process stopped | Skip cache (`cache_hit=false`), call indexer |
| Indexer gRPC | process stopped | Empty candidate set, `failure_reason=indexer_unavailable:...` |
| Privacy gRPC | process stopped | Fail open: keep eligible campaigns, `privacy_reason=privacy_unavailable_fail_open` |
| Kafka broker | `docker compose stop kafka` | Serve still returns ads; impressions/index updates pause until broker is back |
| Kafka produce | broker down | Ad still returned; impression may be missing from analytics |
| Postgres traces | DB down | Ad still returned; replay row not stored |

Indexer Kafka consumer: on error, log, restart consume loop. Campaign writes still land in `campaign_outbox` and retry.

Idempotency: `event_id` is the Kafka message key and primary key of `processed_events` and `campaign_outbox`. Replaying `event123` after `event124` does not re-apply `event123`.

Replay: every serve call has a `request_id`. Inspect `GET http://localhost:8003/v1/traces/{request_id}` after analytics/postgres persist.

Run `scripts/chaos.sh` against a live compose stack.
