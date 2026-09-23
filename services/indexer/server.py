from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager

import grpc
import psycopg
import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest
from pydantic_settings import BaseSettings
from starlette.responses import Response

import adpulse_pb2
import adpulse_pb2_grpc
from index import InvertedIndex

log = logging.getLogger("indexer")
logging.basicConfig(level=logging.INFO)

EVENTS = Counter("adpulse_index_events_total", "Index events", ["result"])
INDEX_SIZE = Gauge("adpulse_index_size", "Campaigns in index")
INDEX_VERSION = Gauge("adpulse_index_version", "Index version")


class Settings(BaseSettings):
    kafka_bootstrap: str = "localhost:19092"
    kafka_topic: str = "campaign.events"
    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql://adpulse:adpulse@localhost:5432/adpulse"
    grpc_port: int = 50051
    http_port: int = 8002
    consumer_group: str = "adpulse-indexer"


settings = Settings()
index = InvertedIndex()
redis_client: redis.Redis | None = None


async def persist_campaign(campaign: dict) -> None:
    if redis_client is None:
        return
    try:
        await redis_client.set(f"campaign:{campaign['id']}", json.dumps(campaign))
    except Exception as exc:
        log.warning("redis write failed: %s", exc)


async def mark_processed(event_id: str) -> bool:
    """Return True if this is the first time we process event_id."""
    conn = await psycopg.AsyncConnection.connect(settings.database_url)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO processed_events (event_id, consumer)
                VALUES (%s, 'indexer')
                ON CONFLICT (event_id) DO NOTHING
                """,
                (event_id,),
            )
            applied = cur.rowcount == 1
        await conn.commit()
        return applied
    except Exception as exc:
        log.warning("processed_events write failed, using in-memory only: %s", exc)
        return True
    finally:
        await conn.close()


async def hydrate_from_postgres() -> None:
    conn = await psycopg.AsyncConnection.connect(settings.database_url)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, advertiser_id, name, headline, landing_url, keywords,
                       bid_micros, daily_budget_micros, spent_today_micros, status,
                       geo_targets, language
                FROM campaigns
                """
            )
            rows = await cur.fetchall()
            for r in rows:
                campaign = {
                    "id": str(r[0]),
                    "advertiser_id": str(r[1]),
                    "name": r[2],
                    "headline": r[3],
                    "landing_url": r[4],
                    "keywords": list(r[5] or []),
                    "bid_micros": r[6],
                    "daily_budget_micros": r[7],
                    "spent_today_micros": r[8],
                    "status": r[9],
                    "geo_targets": list(r[10] or []),
                    "language": r[11],
                }
                event = {"event_id": f"hydrate-{campaign['id']}", "campaign": campaign}
                index.apply_event(event)
                await persist_campaign(campaign)
        INDEX_SIZE.set(len(index.docs))
        INDEX_VERSION.set(index.version)
        log.info("hydrated %s campaigns", len(index.docs))
    finally:
        await conn.close()


async def consume_loop() -> None:
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id=settings.consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode()),
    )
    while True:
        try:
            await consumer.start()
            log.info("kafka consumer started")
            async for msg in consumer:
                event = msg.value
                event_id = event.get("event_id")
                first = await mark_processed(event_id) if event_id else True
                if not first:
                    EVENTS.labels("duplicate").inc()
                    await consumer.commit()
                    continue
                applied = index.apply_event(event)
                if applied:
                    EVENTS.labels("applied").inc()
                    campaign = event.get("campaign")
                    if campaign:
                        await persist_campaign(campaign)
                else:
                    EVENTS.labels("skipped").inc()
                INDEX_SIZE.set(len(index.docs))
                INDEX_VERSION.set(index.version)
                await consumer.commit()
        except Exception as exc:
            log.warning("kafka consume error, retrying: %s", exc)
            EVENTS.labels("error").inc()
            try:
                await consumer.stop()
            except Exception:
                pass
            await asyncio.sleep(2)


class IndexerServicer(adpulse_pb2_grpc.IndexerServicer):
    async def Lookup(self, request, context):
        k = request.max_candidates or 50
        ranked = index.search(request.query, k=k)
        candidates = []
        for cid, rel in ranked:
            doc = index.docs.get(cid)
            if not doc:
                continue
            remaining = max(int(doc.get("daily_budget_micros", 0)) - int(doc.get("spent_today_micros", 0)), 0)
            candidates.append(
                adpulse_pb2.Candidate(
                    campaign_id=cid,
                    relevance=rel,
                    headline=doc.get("headline", ""),
                    landing_url=doc.get("landing_url", ""),
                    bid_micros=int(doc.get("bid_micros", 0)),
                    remaining_budget_micros=remaining,
                    geo_targets=doc.get("geo_targets", []),
                    status=doc.get("status", ""),
                    keywords=doc.get("keywords", []),
                    advertiser_id=str(doc.get("advertiser_id", "")),
                )
            )
        return adpulse_pb2.LookupResponse(
            candidates=candidates,
            index_size=len(index.docs),
            index_version=index.version,
        )


@asynccontextmanager
async def lifespan(_: FastAPI):
    global redis_client
    redis_client = redis.from_url(settings.redis_url)
    try:
        await hydrate_from_postgres()
    except Exception as exc:
        log.warning("hydrate failed: %s", exc)
    consume_task = asyncio.create_task(consume_loop())
    grpc_server = grpc.aio.server()
    adpulse_pb2_grpc.add_IndexerServicer_to_server(IndexerServicer(), grpc_server)
    grpc_server.add_insecure_port(f"0.0.0.0:{settings.grpc_port}")
    await grpc_server.start()
    yield
    consume_task.cancel()
    await grpc_server.stop(grace=2)


app = FastAPI(title="AdPulse Indexer", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "index_size": len(index.docs), "version": index.version}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/v1/debug/search")
async def debug_search(q: str, k: int = 10):
    ranked = index.search(q, k=k)
    return {
        "query": q,
        "results": [{"campaign_id": cid, "relevance": rel, "doc": index.docs.get(cid)} for cid, rel in ranked],
    }


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=settings.http_port)


if __name__ == "__main__":
    main()
