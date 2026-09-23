# Query optimization

Indexes live in `sql/002_indexes.sql`.

## Workload

Serving does **not** hit Postgres on the hot path. Campaign list/filter APIs and indexer hydration do.

The two queries we optimize:

1. Active campaigns whose keyword array contains a token (`= ANY (keywords)`), helped by `GIN (keywords)`.
2. Headline substring search, helped by `pg_trgm` GIN on `headline`.

## How to capture a real before/after

1. Start Postgres: `docker compose up -d postgres`
2. Connect and **drop** the GIN indexes:

```sql
DROP INDEX IF EXISTS idx_campaigns_keywords_gin;
DROP INDEX IF EXISTS idx_campaigns_headline_trgm;
DROP INDEX IF EXISTS idx_campaigns_status;
```

3. Run `make explain` and save the `EXPLAIN ANALYZE` output (look for `Seq Scan` and the `Execution Time`).
4. Recreate indexes from `sql/002_indexes.sql`.
5. `make explain` again. Bitmap Index Scan / Index Scan should appear and execution time should drop as the table grows.

Paste **measured** times into the table below. Leave cells blank until you run the commands.

| Query | Plan before | Time before | Plan after | Time after |
|---|---|---|---|---|
| keyword containment | | | | |
| headline `ILIKE` | | | | |

Seed data is tiny, so the difference is small until you load more campaigns. Use `scripts/` or the campaign API to insert thousands of rows before treating this as a benchmark.
