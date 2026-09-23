from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import date

import psycopg
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, HTTPException
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from pydantic_settings import BaseSettings
from starlette.responses import Response

log = logging.getLogger("analytics")
logging.basicConfig(level=logging.INFO)

EVENTS = Counter("adpulse_analytics_events_total", "Analytics events", ["result"])


class Settings(BaseSettings):
    database_url: str = "postgresql://adpulse:adpulse@localhost:5432/adpulse"
    kafka_bootstrap: str = "localhost:19092"
    impressions_topic: str = "ad.impressions"
    http_port: int = 8003
    consumer_group: str = "adpulse-analytics"


settings = Settings()


async def persist(event: dict) -> None:
    conn = await psycopg.AsyncConnection.connect(settings.database_url)
    try:
        async with conn.cursor() as cur:
            event_id = event.get("event_id")
            if event_id:
                await cur.execute(
                    """
                    INSERT INTO processed_events (event_id, consumer)
                    VALUES (%s, 'analytics')
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (event_id,),
                )
                if cur.rowcount != 1:
                    EVENTS.labels("duplicate").inc()
                    await conn.commit()
                    return
            await cur.execute(
                """
                INSERT INTO impression_events (
                    request_id, campaign_id, query, bid_micros, charged_micros, relevance
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    event.get("request_id"),
                    event.get("campaign_id"),
                    event.get("query"),
                    event.get("bid_micros"),
                    event.get("charged_micros"),
                    event.get("relevance"),
                ),
            )
            await cur.execute(
                """
                INSERT INTO daily_campaign_stats (day, campaign_id, impressions, spend_micros)
                VALUES (%s, %s, 1, %s)
                ON CONFLICT (day, campaign_id) DO UPDATE SET
                    impressions = daily_campaign_stats.impressions + 1,
                    spend_micros = daily_campaign_stats.spend_micros + EXCLUDED.spend_micros
                """,
                (date.today(), event.get("campaign_id"), event.get("charged_micros") or 0),
            )
        await conn.commit()
        EVENTS.labels("applied").inc()
    finally:
        await conn.close()


async def consume_loop() -> None:
    consumer = AIOKafkaConsumer(
        settings.impressions_topic,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id=settings.consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode()),
    )
    while True:
        try:
            await consumer.start()
            async for msg in consumer:
                try:
                    await persist(msg.value)
                    await consumer.commit()
                except Exception as exc:
                    EVENTS.labels("error").inc()
                    log.warning("persist failed: %s", exc)
                    await asyncio.sleep(0.5)
        except Exception as exc:
            log.warning("kafka error: %s", exc)
            try:
                await consumer.stop()
            except Exception:
                pass
            await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(consume_loop())
    yield
    task.cancel()


app = FastAPI(title="AdPulse Analytics", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/v1/stats")
async def stats():
    conn = await psycopg.AsyncConnection.connect(settings.database_url)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT campaign_id, impressions, spend_micros
                FROM daily_campaign_stats
                WHERE day = CURRENT_DATE
                ORDER BY impressions DESC
                """
            )
            rows = await cur.fetchall()
            return [
                {"campaign_id": str(r[0]), "impressions": r[1], "spend_micros": r[2]}
                for r in rows
            ]
    finally:
        await conn.close()


@app.get("/v1/traces/{request_id}")
async def get_trace(request_id: str):
    conn = await psycopg.AsyncConnection.connect(settings.database_url)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT request_id, query, candidate_count, eligible_count,
                       selected_campaign_id, scores, latency_ms, cache_hit, failure_reason, created_at
                FROM request_traces WHERE request_id = %s
                """,
                (request_id,),
            )
            row = await cur.fetchone()
            if not row:
                raise HTTPException(404, "trace not found")
            return {
                "request_id": str(row[0]),
                "query": row[1],
                "candidate_count": row[2],
                "eligible_count": row[3],
                "selected_campaign_id": str(row[4]) if row[4] else None,
                "scores": row[5],
                "latency_ms": row[6],
                "cache_hit": row[7],
                "failure_reason": row[8],
                "created_at": row[9].isoformat(),
            }
    finally:
        await conn.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=settings.http_port)
