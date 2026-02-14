# Phase 0 — Foundations (write these first)

**0.1 `pipelines/common/settings.py`**

Purpose: single source of truth for env vars and configs.
Must contain:

- `Settings` object (class or dict) that loads env vars

- groups: POLYGON_*, KAFKA_*, SNOWFLAKE_*, ALERT_*

- validation: required vars raise clear error
Depends on: nothing.

**0.2 `pipelines/common/logging.py`**

Purpose: consistent structured logs everywhere.
Must contain:

- get_logger(name) returning configured logger

- log format includes timestamp, level, module, correlation_id (optional)
Depends on: settings.py (optional).

**0.3 `pipelines/common/exceptions.py`**

Purpose: typed errors for clear failure handling.
Must contain:

- `ConfigError`, `ExternalServiceError`, `SchemaValidationError`
Depends on: nothing.

**0.4 `pipelines/ingestion/schemas.py`**

Purpose: data contracts for Kafka messages.
Must contain:

- event schemas for bars and news

- `validate_event(event) -> event` (raises SchemaValidationError)

- include schema_version, event_time, symbol, source
Depends on: exceptions.py.

✅ After Phase 0, you can build everything without rewriting constants.

Phase 1 — Kafka + Polygon ingestion (producer side)
1.1 pipelines/ingestion/polygon_client.py

Purpose: a clean API wrapper.
Must contain:

PolygonClient(api_key)

methods:

get_bars(symbol, timespan, start, end)

get_news(symbol, start, end)

retry with backoff, timeout, rate-limit handling
Depends on: settings.py, logging.py, exceptions.py.

1.2 pipelines/ingestion/producer.py

Purpose: publish validated events to Kafka topics.
Must contain:

KafkaProducerClient(bootstrap_servers, …)

send(topic, key, value) with JSON serialization

send_bars(events), send_news(events)

produce message keys as symbol for partition stability
Depends on: settings.py, logging.py, schemas.py.

1.3 pipelines/ingestion/backfill.py

Purpose: orchestration-friendly ingestion entrypoint.
Must contain:

backfill_bars(symbols, start, end, timespan)

backfill_news(symbols, start, end)

returns counts + metrics
Depends on: polygon_client.py, producer.py, settings.py, logging.py.

✅ At this point you can run a local script to push events into Kafka.

Phase 2 — Streaming processor (filter, enrich, route)
2.1 pipelines/streaming/rules.py

Purpose: purely business logic for “interesting events.”
Must contain:

should_keep_bar(bar) -> bool

detect_alerts(bar, state) -> list[alert]

should_keep_news(news) -> bool

detect_news_alerts(news, state) -> list[alert]

thresholds loaded from settings
Depends on: settings.py (and nothing else ideally).

2.2 pipelines/streaming/enrich.py

Purpose: compute rolling metrics and enrich messages.
Must contain:

rolling volume avg, rolling volatility, vwap proxy

join news with symbol metadata if you have it

output “metric events” (for Snowflake metrics table)
Depends on: settings.py, maybe logging.py.

2.3 pipelines/streaming/processor.py

Purpose: runtime that consumes Kafka and produces outputs.
Must contain:

Kafka consumer loop

parse + validate input using schemas.py

apply rules.py and enrich.py

publish to output topics:

market.bars.filtered

market.news.filtered

market.alerts

market.metrics
Depends on: schemas.py, rules.py, enrich.py, logging.py, settings.py.

2.4 pipelines/streaming/checkpointing.py

Purpose: offset behavior / idempotency strategy (simple first).
Must contain:

choose “at least once” with dedupe key

dedupe key format: symbol + event_time + event_type
Depends on: settings.py.

✅ Now you have a streaming “decision engine.”

Phase 3 — Snowflake loading (RAW layer)
3.1 pipelines/warehouse/ddl/raw_tables.sql

Purpose: create target raw tables.
Must include tables:

raw_bars

raw_news

raw_alerts

raw_metrics
Columns must include:

event_time, ingest_time

symbol

payload (VARIANT) to stay flexible

schema_version, source
Depends on: nothing.

3.2 pipelines/warehouse/snowflake_client.py

Purpose: safe connection + execute helpers.
Must contain:

get_conn()

execute(sql, params=None)

executemany(...)
Depends on: settings.py, logging.py, exceptions.py.

3.3 pipelines/warehouse/loaders.py

Purpose: write processed outputs into Snowflake.
Must contain:

write_raw_bars(events)

write_raw_news(events)

write_raw_alerts(alerts)

write_raw_metrics(metrics)
Implementation suggestion:

insert JSON into payload VARIANT

batch inserts
Depends on: snowflake_client.py, settings.py, logging.py.

✅ At this point you can persist filtered/important events + alerts.

Phase 4 — dbt models (staging → marts)
4.1 dbt/models/staging/stg_raw_bars.sql

Purpose: typed columns out of VARIANT.
Must contain:

select payload:field::type as ...

dedupe by (symbol, event_time) using qualify row_number()
Depends on: Snowflake raw tables.

4.2 dbt/models/staging/stg_raw_news.sql

Same approach.

4.3 dbt/models/marts/fact_alerts.sql

Purpose: analytics table for alerts.
Must contain:

one row per alert

severity, rule_id, symbol, event_time
Depends on: staging.

4.4 dbt/models/marts/fact_intraday_metrics.sql

Purpose: rolling metrics history.
Depends on: staging metrics.

4.5 dbt/snapshots/*.sql

Purpose: SCD2 dims if you maintain symbol metadata.
Optional at first.

✅ Now your “warehouse story” is complete.

Phase 5 — Airflow DAGs (only after pipeline modules work)
5.1 dags/ingestion_polygon_to_kafka.py

Purpose: schedule backfills + incremental ingestion.
Must contain tasks:

backfill_bars (daily catchup or last N minutes)

backfill_news
Calls: functions in pipelines/ingestion/backfill.py
Depends on: ingestion modules.

5.2 dags/dbt_build_daily.py

Purpose: run dbt staging → marts (and snapshots if used).
Tasks:

dbt run --select models/staging

dbt snapshot (optional)

dbt run --select models/marts
Depends on: dbt project.

5.3 dags/stream_to_snowflake.py (optional pattern)

Purpose: monitor streaming job health / restart.
In real companies streaming runs as a service, not an Airflow task.
For your project:

Airflow can run “health checks” (topic lag, last event time)
Depends on: streaming processor logs/metrics.

Dependency graph (simple)

Base

settings.py → used by almost everything

logging.py → used by most modules

schemas.py → used by producer + processor

Ingestion

polygon_client.py → backfill.py

producer.py + schemas.py → backfill.py

backfill.py → Airflow ingestion DAG

Streaming

schemas.py → processor.py

rules.py + enrich.py → processor.py

Warehouse

snowflake_client.py → loaders.py

loaders.py → (used by processor OR a separate consumer)

dbt

depends only on Snowflake raw tables

Airflow

DAGs depend on ingestion modules and dbt commands

The recommended writing order (copy this checklist)

pipelines/common/settings.py

pipelines/common/logging.py

pipelines/common/exceptions.py

pipelines/ingestion/schemas.py

pipelines/ingestion/polygon_client.py

pipelines/ingestion/producer.py

pipelines/ingestion/backfill.py

pipelines/streaming/rules.py

pipelines/streaming/enrich.py

pipelines/streaming/processor.py

pipelines/warehouse/ddl/raw_tables.sql

pipelines/warehouse/snowflake_client.py

pipelines/warehouse/loaders.py

dbt staging models

dbt marts models

Airflow DAGs

One key architecture decision you must choose now

When the stream processor creates filtered/alerts/metrics, do you want it to:

A) Write directly to Snowflake (simpler, fewer services)
Kafka → processor → Snowflake RAW

or

B) Publish to Kafka output topics and have a separate loader (more “enterprise”)
Kafka → processor → Kafka(filtered/alerts/metrics) → loader → Snowflake RAW

If you don’t want to decide, choose A first (you can refactor later).

If you tell me A or B, I’ll tailor the file responsibilities and dependencies to match perfectly.