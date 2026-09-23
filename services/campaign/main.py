import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import psycopg
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from pydantic import BaseModel, Field
from starlette.responses import Response

from settings import settings

log = logging.getLogger("campaign")
logging.basicConfig(level=logging.INFO)

REQUESTS = Counter("adpulse_campaign_requests_total", "Campaign API requests", ["route"])

producer: AIOKafkaProducer | None = None
pool: psycopg.AsyncConnection | None = None


class AdvertiserIn(BaseModel):
    name: str


class CampaignIn(BaseModel):
    advertiser_id: UUID
    name: str
    headline: str
    landing_url: str
    keywords: list[str]
    bid_micros: int = Field(gt=0)
    daily_budget_micros: int = Field(gt=0)
    status: str = "active"
    geo_targets: list[str] = Field(default_factory=list)
    language: str = "en"
    idempotency_key: UUID | None = None


def row_campaign(r: Any) -> dict:
    return {
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
        "updated_at": r[12].isoformat(),
    }


async def get_conn() -> psycopg.AsyncConnection:
    return await psycopg.AsyncConnection.connect(settings.database_url, autocommit=False)


async def ensure_producer() -> AIOKafkaProducer | None:
    global producer
    if producer is not None:
        return producer
    p = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap)
    try:
        await p.start()
        producer = p
        log.info("kafka producer connected")
        return producer
    except Exception as exc:
        log.warning("kafka still unavailable: %s", exc)
        return None


async def publish_outbox(conn: psycopg.AsyncConnection) -> None:
    prod = await ensure_producer()
    if prod is None:
        return
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT event_id, payload FROM campaign_outbox
            WHERE NOT published
            ORDER BY created_at
            LIMIT 100
            FOR UPDATE SKIP LOCKED
            """
        )
        rows = await cur.fetchall()
        for event_id, payload in rows:
            body = json.dumps(payload) if not isinstance(payload, str) else payload
            await prod.send_and_wait(
                settings.kafka_topic,
                body.encode() if isinstance(body, str) else json.dumps(payload).encode(),
                key=str(event_id).encode(),
            )
            await cur.execute(
                "UPDATE campaign_outbox SET published = TRUE WHERE event_id = %s",
                (event_id,),
            )
        await conn.commit()


async def outbox_loop() -> None:
    while True:
        try:
            conn = await get_conn()
            try:
                await publish_outbox(conn)
            finally:
                await conn.close()
        except Exception as exc:
            log.warning("outbox publish failed: %s", exc)
        await asyncio.sleep(0.4)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global producer
    producer = None
    task = asyncio.create_task(outbox_loop())
    yield
    task.cancel()
    if producer:
        await producer.stop()


app = FastAPI(title="AdPulse Campaign Service", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/v1/advertisers")
async def create_advertiser(body: AdvertiserIn):
    REQUESTS.labels("create_advertiser").inc()
    conn = await get_conn()
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO advertisers (name) VALUES (%s) RETURNING id, name, created_at",
                (body.name,),
            )
            row = await cur.fetchone()
            await conn.commit()
            return {"id": str(row[0]), "name": row[1], "created_at": row[2].isoformat()}
    finally:
        await conn.close()


@app.get("/v1/campaigns")
async def list_campaigns(status: str | None = None):
    REQUESTS.labels("list_campaigns").inc()
    conn = await get_conn()
    try:
        async with conn.cursor() as cur:
            if status:
                await cur.execute(
                    """
                    SELECT id, advertiser_id, name, headline, landing_url, keywords,
                           bid_micros, daily_budget_micros, spent_today_micros, status,
                           geo_targets, language, updated_at
                    FROM campaigns WHERE status = %s ORDER BY updated_at DESC
                    """,
                    (status,),
                )
            else:
                await cur.execute(
                    """
                    SELECT id, advertiser_id, name, headline, landing_url, keywords,
                           bid_micros, daily_budget_micros, spent_today_micros, status,
                           geo_targets, language, updated_at
                    FROM campaigns ORDER BY updated_at DESC
                    """
                )
            rows = await cur.fetchall()
            return [row_campaign(r) for r in rows]
    finally:
        await conn.close()


@app.get("/v1/campaigns/{campaign_id}")
async def get_campaign(campaign_id: UUID):
    REQUESTS.labels("get_campaign").inc()
    conn = await get_conn()
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, advertiser_id, name, headline, landing_url, keywords,
                       bid_micros, daily_budget_micros, spent_today_micros, status,
                       geo_targets, language, updated_at
                FROM campaigns WHERE id = %s
                """,
                (campaign_id,),
            )
            row = await cur.fetchone()
            if not row:
                raise HTTPException(404, "campaign not found")
            return row_campaign(row)
    finally:
        await conn.close()


async def write_campaign(conn, body: CampaignIn, campaign_id: UUID, event_type: str):
    event_id = body.idempotency_key or uuid4()
    async with conn.cursor() as cur:
        await cur.execute("SELECT payload FROM campaign_outbox WHERE event_id = %s", (event_id,))
        existing_event = await cur.fetchone()
        if existing_event:
            payload = existing_event[0]
            if isinstance(payload, str):
                import json as _json
                payload = _json.loads(payload)
            return payload["campaign"], True

        now = datetime.now(timezone.utc)
        await cur.execute(
            """
            INSERT INTO campaigns (
                id, advertiser_id, name, headline, landing_url, keywords,
                bid_micros, daily_budget_micros, status, geo_targets, language, updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                headline = EXCLUDED.headline,
                landing_url = EXCLUDED.landing_url,
                keywords = EXCLUDED.keywords,
                bid_micros = EXCLUDED.bid_micros,
                daily_budget_micros = EXCLUDED.daily_budget_micros,
                status = EXCLUDED.status,
                geo_targets = EXCLUDED.geo_targets,
                language = EXCLUDED.language,
                updated_at = EXCLUDED.updated_at
            RETURNING id, advertiser_id, name, headline, landing_url, keywords,
                      bid_micros, daily_budget_micros, spent_today_micros, status,
                      geo_targets, language, updated_at
            """,
            (
                campaign_id,
                body.advertiser_id,
                body.name,
                body.headline,
                body.landing_url,
                body.keywords,
                body.bid_micros,
                body.daily_budget_micros,
                body.status,
                body.geo_targets,
                body.language,
                now,
            ),
        )
        row = await cur.fetchone()
        payload = {
            "event_id": str(event_id),
            "event_type": event_type,
            "campaign": row_campaign(row),
        }
        await cur.execute(
            """
            INSERT INTO campaign_outbox (event_id, campaign_id, event_type, payload)
            VALUES (%s, %s, %s, %s::jsonb)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (event_id, campaign_id, event_type, json.dumps(payload)),
        )
        await conn.commit()
        return row_campaign(row), False


@app.post("/v1/campaigns")
async def create_campaign(body: CampaignIn):
    REQUESTS.labels("create_campaign").inc()
    if body.status not in ("draft", "active", "paused"):
        raise HTTPException(400, "invalid status")
    campaign_id = uuid4()
    conn = await get_conn()
    try:
        campaign, replayed = await write_campaign(conn, body, campaign_id, "upsert")
        return {"replayed": replayed, "campaign": campaign}
    except psycopg.Error as exc:
        await conn.rollback()
        raise HTTPException(400, str(exc)) from exc
    finally:
        await conn.close()


@app.put("/v1/campaigns/{campaign_id}")
async def update_campaign(campaign_id: UUID, body: CampaignIn):
    REQUESTS.labels("update_campaign").inc()
    conn = await get_conn()
    try:
        campaign, replayed = await write_campaign(conn, body, campaign_id, "upsert")
        return {"replayed": replayed, "campaign": campaign}
    finally:
        await conn.close()
