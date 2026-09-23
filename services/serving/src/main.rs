mod auction;
mod eligibility;
mod text;

use std::sync::Arc;
use std::time::Instant;

use auction::{run_auction, AuctionCandidate};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use eligibility::eligible;
use once_cell::sync::Lazy;
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, TextEncoder};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;
use uuid::Uuid;

pub mod pb {
    tonic::include_proto!("adpulse");
}

use pb::indexer_client::IndexerClient;
use pb::privacy_client::PrivacyClient;
use pb::{LookupRequest, PrivacyRequest};

static SERVE_REQ: Lazy<IntCounter> =
    Lazy::new(|| IntCounter::new("adpulse_serve_requests_total", "serve requests").unwrap());
static SERVE_ERR: Lazy<IntCounter> =
    Lazy::new(|| IntCounter::new("adpulse_serve_errors_total", "serve errors").unwrap());
static CACHE_HIT: Lazy<IntCounter> =
    Lazy::new(|| IntCounter::new("adpulse_cache_hits_total", "cache hits").unwrap());
static CACHE_MISS: Lazy<IntCounter> =
    Lazy::new(|| IntCounter::new("adpulse_cache_misses_total", "cache misses").unwrap());
static SERVE_LAT: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(HistogramOpts::new(
        "adpulse_serve_latency_seconds",
        "serve latency",
    ))
    .unwrap()
});

#[derive(Clone)]
struct AppState {
    redis: Option<ConnectionManager>,
    pg: Option<Arc<tokio_postgres::Client>>,
    kafka: Option<FutureProducer>,
    indexer: String,
    privacy: String,
}

#[derive(Deserialize)]
struct ServeRequest {
    query: String,
    request_id: Option<Uuid>,
    geo: Option<String>,
    cohort_id: Option<String>,
}

#[derive(Serialize, Clone)]
struct ScoreRow {
    campaign_id: String,
    relevance: f64,
    bid_micros: i64,
    score: f64,
    eligibility: String,
}

#[derive(Serialize)]
struct AdPayload {
    campaign_id: String,
    headline: String,
    landing_url: String,
    charged_micros: i64,
    relevance: f64,
}

#[derive(Serialize)]
struct ServeResponse {
    request_id: Uuid,
    query: String,
    ad: Option<AdPayload>,
    candidate_count: usize,
    eligible_count: usize,
    scores: Vec<ScoreRow>,
    latency_ms: u128,
    cache_hit: bool,
    privacy_reason: String,
    failure_reason: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();
    prometheus::default_registry()
        .register(Box::new(SERVE_REQ.clone()))
        .ok();
    prometheus::default_registry()
        .register(Box::new(SERVE_ERR.clone()))
        .ok();
    prometheus::default_registry()
        .register(Box::new(CACHE_HIT.clone()))
        .ok();
    prometheus::default_registry()
        .register(Box::new(CACHE_MISS.clone()))
        .ok();
    prometheus::default_registry()
        .register(Box::new(SERVE_LAT.clone()))
        .ok();

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".into());
    let redis = match redis::Client::open(redis_url) {
        Ok(c) => match ConnectionManager::new(c).await {
            Ok(m) => Some(m),
            Err(e) => {
                tracing::warn!("redis unavailable: {e}");
                None
            }
        },
        Err(e) => {
            tracing::warn!("redis client error: {e}");
            None
        }
    };

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://adpulse:adpulse@localhost:5432/adpulse".into());
    let pg = match tokio_postgres::connect(&db_url, NoTls).await {
        Ok((client, conn)) => {
            tokio::spawn(async move {
                let _ = conn.await;
            });
            Some(Arc::new(client))
        }
        Err(e) => {
            tracing::warn!("postgres unavailable: {e}");
            None
        }
    };

    let kafka_bootstrap =
        std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:19092".into());
    let kafka = ClientConfig::new()
        .set("bootstrap.servers", &kafka_bootstrap)
        .set("message.timeout.ms", "1500")
        .create()
        .ok();

    let state = AppState {
        redis,
        pg,
        kafka,
        indexer: std::env::var("INDEXER_GRPC").unwrap_or_else(|_| "http://127.0.0.1:50051".into()),
        privacy: std::env::var("PRIVACY_GRPC").unwrap_or_else(|_| "http://127.0.0.1:50052".into()),
    };

    let port = std::env::var("HTTP_PORT").unwrap_or_else(|_| "8000".into());
    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/v1/ads/serve", post(serve))
        .with_state(Arc::new(state));

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("bind");
    tracing::info!("serving on {port}");
    axum::serve(listener, app).await.expect("server");
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status":"ok"}))
}

async fn metrics() -> impl IntoResponse {
    let mut buf = Vec::new();
    let encoder = TextEncoder::new();
    encoder.encode(&prometheus::gather(), &mut buf).ok();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        buf,
    )
}

async fn serve(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ServeRequest>,
) -> impl IntoResponse {
    let start = Instant::now();
    SERVE_REQ.inc();
    let request_id = req.request_id.unwrap_or_else(Uuid::new_v4);
    let query = req.query.clone();
    let mut cache_hit = false;
    let mut failure_reason: Option<String> = None;
    let mut privacy_reason = "unspecified".to_string();

    let lookup = match cached_or_lookup(&state, &query, request_id).await {
        Ok((cands, hit)) => {
            cache_hit = hit;
            if hit {
                CACHE_HIT.inc();
            } else {
                CACHE_MISS.inc();
            }
            cands
        }
        Err(e) => {
            SERVE_ERR.inc();
            failure_reason = Some(format!("indexer_unavailable:{e}"));
            Vec::new()
        }
    };

    let candidate_count = lookup.len();
    let geo = req.geo.as_deref();
    let mut scores = Vec::new();
    let mut auction_input = Vec::new();
    for c in lookup {
        match eligible(&c.status, c.remaining_budget_micros, &c.geo_targets, geo) {
            Ok(()) => {
                scores.push(ScoreRow {
                    campaign_id: c.campaign_id.clone(),
                    relevance: c.relevance,
                    bid_micros: c.bid_micros,
                    score: auction::score(c.relevance, c.bid_micros),
                    eligibility: "ok".into(),
                });
                auction_input.push(c);
            }
            Err(reason) => scores.push(ScoreRow {
                campaign_id: c.campaign_id.clone(),
                relevance: c.relevance,
                bid_micros: c.bid_micros,
                score: 0.0,
                eligibility: reason.into(),
            }),
        }
    }

    let campaign_ids: Vec<String> = auction_input.iter().map(|c| c.campaign_id.clone()).collect();
    match privacy_filter(&state, request_id, req.cohort_id.as_deref(), &campaign_ids).await {
        Ok((allowed, reason)) => {
            privacy_reason = reason;
            auction_input.retain(|c| allowed.contains(&c.campaign_id));
        }
        Err(e) => {
            privacy_reason = format!("privacy_unavailable_fail_open:{e}");
        }
    }

    let eligible_count = auction_input.len();
    let result = run_auction(
        auction_input
            .into_iter()
            .map(|c| AuctionCandidate {
                campaign_id: c.campaign_id,
                headline: c.headline,
                landing_url: c.landing_url,
                relevance: c.relevance,
                bid_micros: c.bid_micros,
            })
            .collect(),
    );

    if result.is_none() && failure_reason.is_none() {
        failure_reason = Some("no_eligible_campaigns".into());
    }

    let ad = result.as_ref().map(|r| AdPayload {
        campaign_id: r.winner.campaign_id.clone(),
        headline: r.winner.headline.clone(),
        landing_url: r.winner.landing_url.clone(),
        charged_micros: r.charged_micros,
        relevance: r.winner.relevance,
    });

    let latency_ms = start.elapsed().as_millis();
    SERVE_LAT.observe(start.elapsed().as_secs_f64());

    write_trace(
        &state,
        request_id,
        &query,
        candidate_count,
        eligible_count,
        ad.as_ref().map(|a| a.campaign_id.clone()),
        &scores,
        latency_ms as i32,
        cache_hit,
        failure_reason.as_deref(),
    )
    .await;

    if let Some(r) = result.as_ref() {
        publish_impression(&state, request_id, &query, r).await;
    }

    Json(ServeResponse {
        request_id,
        query,
        ad,
        candidate_count,
        eligible_count,
        scores,
        latency_ms,
        cache_hit,
        privacy_reason,
        failure_reason,
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CachedCandidate {
    campaign_id: String,
    relevance: f64,
    headline: String,
    landing_url: String,
    bid_micros: i64,
    remaining_budget_micros: i64,
    geo_targets: Vec<String>,
    status: String,
    keywords: Vec<String>,
}

async fn cached_or_lookup(
    state: &AppState,
    query: &str,
    request_id: Uuid,
) -> Result<(Vec<CachedCandidate>, bool), String> {
    let key = format!("q:{}", text::normalize(query));
    if let Some(redis) = &state.redis {
        let mut r = redis.clone();
        let cached: Result<Option<String>, _> =
            redis::cmd("GET").arg(&key).query_async(&mut r).await;
        if let Ok(Some(val)) = cached {
            if let Ok(cands) = serde_json::from_str::<Vec<CachedCandidate>>(&val) {
                return Ok((cands, true));
            }
        }
    }

    let mut client = IndexerClient::connect(state.indexer.clone())
        .await
        .map_err(|e| e.to_string())?;
    let resp = client
        .lookup(LookupRequest {
            query: query.to_string(),
            request_id: request_id.to_string(),
            max_candidates: 50,
        })
        .await
        .map_err(|e| e.to_string())?
        .into_inner();

    let cands: Vec<CachedCandidate> = resp
        .candidates
        .into_iter()
        .map(|c| CachedCandidate {
            campaign_id: c.campaign_id,
            relevance: c.relevance,
            headline: c.headline,
            landing_url: c.landing_url,
            bid_micros: c.bid_micros,
            remaining_budget_micros: c.remaining_budget_micros,
            geo_targets: c.geo_targets,
            status: c.status,
            keywords: c.keywords,
        })
        .collect();

    if let Some(redis) = &state.redis {
        let mut r = redis.clone();
        if let Ok(payload) = serde_json::to_string(&cands) {
            let _: Result<(), _> = redis::cmd("SETEX")
                .arg(&key)
                .arg(15)
                .arg(payload)
                .query_async(&mut r)
                .await;
        }
    }
    Ok((cands, false))
}

async fn privacy_filter(
    state: &AppState,
    request_id: Uuid,
    cohort_id: Option<&str>,
    campaign_ids: &[String],
) -> Result<(Vec<String>, String), String> {
    let mut client = PrivacyClient::connect(state.privacy.clone())
        .await
        .map_err(|e| e.to_string())?;
    let resp = client
        .filter(PrivacyRequest {
            request_id: request_id.to_string(),
            cohort_id: cohort_id.unwrap_or("").to_string(),
            campaign_ids: campaign_ids.to_vec(),
        })
        .await
        .map_err(|e| e.to_string())?
        .into_inner();
    Ok((resp.allowed_campaign_ids, resp.reason))
}

async fn write_trace(
    state: &AppState,
    request_id: Uuid,
    query: &str,
    candidate_count: usize,
    eligible_count: usize,
    selected: Option<String>,
    scores: &[ScoreRow],
    latency_ms: i32,
    cache_hit: bool,
    failure_reason: Option<&str>,
) {
    let Some(pg) = &state.pg else {
        return;
    };
    let scores_json = serde_json::to_string(scores).unwrap_or_else(|_| "[]".into());
    let rid = request_id.to_string();
    let selected_s = selected.unwrap_or_default();
    let fail = failure_reason.unwrap_or("");
    let _ = pg
        .execute(
            "INSERT INTO request_traces (
                request_id, query, candidate_count, eligible_count, selected_campaign_id,
                scores, latency_ms, cache_hit, failure_reason
            ) VALUES ($1::uuid,$2,$3,$4,NULLIF($5,'')::uuid,$6::jsonb,$7,$8,NULLIF($9,''))
            ON CONFLICT (request_id) DO NOTHING",
            &[
                &rid,
                &query,
                &(candidate_count as i32),
                &(eligible_count as i32),
                &selected_s,
                &scores_json,
                &latency_ms,
                &cache_hit,
                &fail,
            ],
        )
        .await;
}

async fn publish_impression(
    state: &AppState,
    request_id: Uuid,
    query: &str,
    result: &auction::AuctionResult,
) {
    let Some(producer) = &state.kafka else {
        return;
    };
    let event_id = Uuid::new_v4();
    let payload = serde_json::json!({
        "event_id": event_id,
        "request_id": request_id,
        "campaign_id": result.winner.campaign_id,
        "query": query,
        "bid_micros": result.winner.bid_micros,
        "charged_micros": result.charged_micros,
        "relevance": result.winner.relevance,
    });
    let body = payload.to_string();
    let key = event_id.to_string();
    let record = FutureRecord::to("ad.impressions")
        .payload(&body)
        .key(&key);
    let _ = producer.send(record, std::time::Duration::from_millis(500)).await;
}
