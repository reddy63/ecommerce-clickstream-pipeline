# Data Dictionary — Ecommerce Clickstream Pipeline

## Overview

This document defines all columns across the pipeline's data layers: Bronze, Silver, Gold, and dbt Marts.

---

## Bronze Layer

**Location:** `hdfs://namenode:9000/data/bronze`
**Format:** Parquet
**Source:** Spark Streaming from Kafka topic `clickstream`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `product_id` | INTEGER | No | Product ID from Fake Store API (1–20) |
| `category` | STRING | No | Product category: `electronics`, `jewelery`, `men's clothing`, `women's clothing` |
| `price` | DOUBLE | No | Product price in USD |
| `title` | STRING | Yes | Product title |
| `description` | STRING | Yes | Product description |
| `rating_rate` | DOUBLE | Yes | Product rating score |
| `rating_count` | INTEGER | Yes | Number of product ratings |
| `event_ts` | LONG | No | Unix timestamp when the event was produced |

---

## Silver Layer

**Location:** `hdfs://namenode:9000/data/silver`
**Format:** Parquet, partitioned by `event_date`
**Source:** Spark batch `silver_clickstream.py` from Bronze

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `product_id` | INTEGER | No | Product ID from Fake Store API |
| `category` | STRING | No | Product category |
| `price` | DOUBLE | No | Product price in USD |
| `title` | STRING | Yes | Product title |
| `description` | STRING | Yes | Product description |
| `rating_rate` | DOUBLE | Yes | Product rating score |
| `rating_count` | INTEGER | Yes | Number of product ratings |
| `event_ts` | LONG | No | Original Unix timestamp |
| `event_time` | TIMESTAMP | No | Parsed timestamp from `event_ts` |
| `event_date` | DATE | No | Date extracted from `event_time` (partition key) |

**Transformations applied:**
- Deduplicated by `(product_id, event_ts)` using row_number window function
- `event_time` derived via `to_timestamp(from_unixtime(event_ts))`
- `event_date` derived via `to_date(event_time)`

---

## Gold Layer

**Location:** `hdfs://namenode:9000/data/gold`
**Format:** Parquet, partitioned by `event_date`
**Source:** Spark batch `gold_clickstream.py` from Silver

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `category` | STRING | No | Product category |
| `event_date` | DATE | No | Aggregation date (partition key) |
| `total_events` | LONG | No | Count of events per category per day |
| `total_revenue` | DOUBLE | No | Sum of prices per category per day |

---

## PostgreSQL — `clickstream_metrics`

**Database:** `analytics` on external container `postgre`
**Source:** Spark batch `gold_to_postgres.py` from Gold

Same schema as Gold layer above.

---

## dbt Staging — `stg_clickstream`

**Database:** `analytics`, **Schema:** `staging`
**Materialization:** View
**Source:** dbt from `clickstream_metrics` table

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `category` | STRING | No | Product category |
| `event_date` | DATE | No | Aggregation date |
| `total_events` | INTEGER | No | Count of events (coalesced to 0 if null) |
| `total_revenue` | NUMERIC | No | Sum of revenue (coalesced to 0 if null) |
| `avg_price_per_event` | NUMERIC | Yes | `total_revenue / total_events`, rounded to 2 decimals |
| `dbt_loaded_at` | TIMESTAMP | No | Timestamp when dbt processed the record |

---

## dbt Marts — `fct_category_metrics`

**Database:** `analytics`, **Schema:** `marts`
**Materialization:** Table

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `category` | STRING | No | Product category |
| `event_date` | DATE | No | Aggregation date |
| `total_events` | INTEGER | No | Event count |
| `total_revenue` | NUMERIC | No | Revenue sum |
| `avg_price_per_event` | NUMERIC | Yes | Average price per event |
| `revenue_pct_of_day` | NUMERIC | Yes | % of total daily revenue this category represents |
| `revenue_rank` | INTEGER | No | Rank by revenue within the day (1 = highest) |
| `dbt_updated_at` | TIMESTAMP | No | Last dbt refresh timestamp |

---

## dbt Marts — `fct_daily_revenue`

**Database:** `analytics`, **Schema:** `marts`
**Materialization:** Table

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `event_date` | DATE | No | Aggregation date (unique) |
| `unique_categories` | INTEGER | No | Distinct categories active on this date |
| `total_events` | INTEGER | No | Total events across all categories |
| `total_revenue` | NUMERIC | No | Total revenue across all categories |
| `avg_price_per_event` | NUMERIC | Yes | Average price per event across all categories |
| `top_category_revenue` | NUMERIC | Yes | Revenue of the highest-earning category |
| `dbt_updated_at` | TIMESTAMP | No | Last dbt refresh timestamp |
