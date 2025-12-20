# Goal

Build an end-to-end cloud data pipeline that ingests NYC TLC taxi trip data from a public API into Snowflake using AWS S3 as the raw landing zone, transforms with dbt, and orchestrates with Airflow. The pipeline is designed with production-like constraints (SLA, retries, backfills, schema drift tolerance).

# Data source

NYC TLC taxi trip data via public API (JSON).
Pull scope: one trip type first (e.g., “yellow taxi trips”), then expand.

# Schedule & SLA

Schedule: Daily (e.g., runs at 5:00 AM)

SLA: Gold tables ready by 8:00 AM local time

Backfills: Supported for any date range

# Storage design

Raw: S3 s3://<bucket>/tlc/raw/dt=YYYY-MM-DD/…json

Staging: S3 s3://<bucket>/tlc/stage/dt=YYYY-MM-DD/…parquet|json

Warehouse: Snowflake

RAW_TLC_* tables (minimally processed)

BRONZE_*, SILVER_*, GOLD_* modeled via dbt

# Incremental strategy

Watermark by date partition (dt)

Each daily run fetches dt = yesterday (or configurable)

Idempotent writes: rerun replaces that day’s partition outputs

# Data quality (minimum viable)

Row count > 0 for expected run dates

Required fields not null where appropriate

Primary key uniqueness strategy in SILVER (or dedupe rules)

# Schema drift strategy

Persist raw payloads (never lose data)

In Silver: cast/select known fields; unknown fields are tolerated

Add a “schema changes detected” log entry when field set changes vs baseline

# Failure handling

API errors: retry with exponential backoff

Partial loads: fail the task and allow rerun

Snowflake load failures: stop downstream tasks

dbt test failures: fail pipeline (surfaced clearly)

# Cost awareness

Minimize Snowflake compute by:

loading once per day

using incremental models

limiting dev runs to small date ranges

# Definition of Done (by early Feb)

One-click local run instructions

Airflow DAG runs end-to-end for at least 7 consecutive daily partitions

dbt models + tests produce Gold tables

Docs: architecture, runbook, decisions, failures 