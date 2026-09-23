CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE advertisers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE campaigns (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    advertiser_id UUID NOT NULL REFERENCES advertisers (id),
    name TEXT NOT NULL,
    headline TEXT NOT NULL,
    landing_url TEXT NOT NULL,
    keywords TEXT[] NOT NULL DEFAULT '{}',
    bid_micros BIGINT NOT NULL CHECK (bid_micros > 0),
    daily_budget_micros BIGINT NOT NULL CHECK (daily_budget_micros > 0),
    spent_today_micros BIGINT NOT NULL DEFAULT 0 CHECK (spent_today_micros >= 0),
    status TEXT NOT NULL CHECK (status IN ('draft', 'active', 'paused')),
    geo_targets TEXT[] NOT NULL DEFAULT '{}',
    language TEXT NOT NULL DEFAULT 'en',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE campaign_outbox (
    event_id UUID PRIMARY KEY,
    campaign_id UUID NOT NULL REFERENCES campaigns (id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE processed_events (
    event_id UUID PRIMARY KEY,
    consumer TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE request_traces (
    request_id UUID PRIMARY KEY,
    query TEXT NOT NULL,
    candidate_count INT NOT NULL DEFAULT 0,
    eligible_count INT NOT NULL DEFAULT 0,
    selected_campaign_id UUID,
    scores JSONB,
    latency_ms INT,
    cache_hit BOOLEAN NOT NULL DEFAULT FALSE,
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE impression_events (
    id BIGSERIAL PRIMARY KEY,
    request_id UUID NOT NULL,
    campaign_id UUID,
    query TEXT,
    bid_micros BIGINT,
    charged_micros BIGINT,
    relevance DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE daily_campaign_stats (
    day DATE NOT NULL,
    campaign_id UUID NOT NULL,
    impressions BIGINT NOT NULL DEFAULT 0,
    clicks BIGINT NOT NULL DEFAULT 0,
    spend_micros BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, campaign_id)
);

CREATE TABLE cohort_counts (
    cohort_id TEXT PRIMARY KEY,
    member_count INT NOT NULL CHECK (member_count >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
