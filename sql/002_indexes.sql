-- Query optimization: these indexes are applied after the unindexed baseline
-- documented in docs/query_optimization.md

CREATE INDEX idx_campaigns_status ON campaigns (status);
CREATE INDEX idx_campaigns_advertiser ON campaigns (advertiser_id);
CREATE INDEX idx_campaigns_updated_at ON campaigns (updated_at DESC);
CREATE INDEX idx_campaigns_keywords_gin ON campaigns USING GIN (keywords);
CREATE INDEX idx_campaigns_geo_gin ON campaigns USING GIN (geo_targets);
CREATE INDEX idx_campaigns_headline_trgm ON campaigns USING GIN (headline gin_trgm_ops);

CREATE INDEX idx_outbox_unpublished ON campaign_outbox (created_at) WHERE NOT published;
CREATE INDEX idx_processed_consumer ON processed_events (consumer, processed_at);
CREATE INDEX idx_traces_created ON request_traces (created_at DESC);
CREATE INDEX idx_impressions_campaign_day ON impression_events (campaign_id, created_at);
CREATE INDEX idx_impressions_request ON impression_events (request_id);
