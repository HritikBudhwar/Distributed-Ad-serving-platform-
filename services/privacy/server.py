from __future__ import annotations

import hashlib
import logging
from contextlib import asynccontextmanager

import grpc
import psycopg
from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from pydantic_settings import BaseSettings
from starlette.responses import Response

import adpulse_pb2
import adpulse_pb2_grpc

log = logging.getLogger("privacy")
logging.basicConfig(level=logging.INFO)

FILTERS = Counter("adpulse_privacy_filters_total", "Privacy filter calls", ["result"])


class Settings(BaseSettings):
    database_url: str = "postgresql://adpulse:adpulse@localhost:5432/adpulse"
    k_threshold: int = 50
    grpc_port: int = 50052
    http_port: int = 8004


settings = Settings()


def pseudonymize(cohort_id: str) -> str:
    if not cohort_id:
        return ""
    return hashlib.sha256(cohort_id.encode()).hexdigest()[:16]


async def cohort_size(cohort_id: str) -> int:
    if not cohort_id:
        return 0
    conn = await psycopg.AsyncConnection.connect(settings.database_url)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT member_count FROM cohort_counts WHERE cohort_id = %s",
                (cohort_id,),
            )
            row = await cur.fetchone()
            return int(row[0]) if row else 0
    except Exception as exc:
        log.warning("cohort lookup failed: %s", exc)
        return 0
    finally:
        await conn.close()


class PrivacyServicer(adpulse_pb2_grpc.PrivacyServicer):
    async def Filter(self, request, context):
        cid = request.cohort_id or ""
        size = await cohort_size(cid)
        eligible = size >= settings.k_threshold
        if not cid:
            FILTERS.labels("no_cohort").inc()
            return adpulse_pb2.PrivacyResponse(
                allowed_campaign_ids=list(request.campaign_ids),
                cohort_eligible=True,
                reason="no_cohort_contextual_only",
                cohort_size=0,
                k_threshold=settings.k_threshold,
            )
        if not eligible:
            FILTERS.labels("k_blocked").inc()
            return adpulse_pb2.PrivacyResponse(
                allowed_campaign_ids=list(request.campaign_ids),
                cohort_eligible=False,
                reason="below_k_threshold_contextual_only",
                cohort_size=size,
                k_threshold=settings.k_threshold,
            )
        FILTERS.labels("allowed").inc()
        return adpulse_pb2.PrivacyResponse(
            allowed_campaign_ids=list(request.campaign_ids),
            cohort_eligible=True,
            reason="cohort_k_anonymous",
            cohort_size=size,
            k_threshold=settings.k_threshold,
        )


@asynccontextmanager
async def lifespan(_: FastAPI):
    server = grpc.aio.server()
    adpulse_pb2_grpc.add_PrivacyServicer_to_server(PrivacyServicer(), server)
    server.add_insecure_port(f"0.0.0.0:{settings.grpc_port}")
    await server.start()
    yield
    await server.stop(2)


app = FastAPI(title="AdPulse Privacy", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "k_threshold": settings.k_threshold}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/v1/cohorts/{cohort_id}")
async def get_cohort(cohort_id: str):
    size = await cohort_size(cohort_id)
    return {
        "cohort_id": cohort_id,
        "pseudonym": pseudonymize(cohort_id),
        "member_count": size,
        "eligible": size >= settings.k_threshold,
        "k_threshold": settings.k_threshold,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=settings.http_port)
