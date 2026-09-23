-- Run with: make explain
-- Compare Seq Scan vs index-backed plans. Capture this output in docs/query_optimization.md
-- after you run it; do not copy fabricated timings.

EXPLAIN ANALYZE
SELECT id, headline, bid_micros
FROM campaigns
WHERE status = 'active' AND 'running' = ANY (keywords);

EXPLAIN ANALYZE
SELECT id, headline
FROM campaigns
WHERE headline ILIKE '%running%';
