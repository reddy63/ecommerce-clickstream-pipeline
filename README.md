<div align="center">

# Ecommerce Clickstream Pipeline

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Kafka-7.5.0-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-3.5.8-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.x-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-PostgreSQL-FF694B?logo=dbt&logoColor=white)](https://docs.getdbt.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![License: MIT](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

**End-to-end streaming + batch data pipeline with Medallion Architecture.**  
Real-time product events stream through Kafka into HDFS via Spark Streaming (Bronze),  
batch-processed into clean and aggregated layers (Silver → Gold) by Airflow-orchestrated  
Spark jobs, transformed by dbt, and visualised in Metabase.

</div>

---

## Architecture

```
Fake Store API  (https://fakestoreapi.com/products)
      |  REST poll every 30s — 20 product events per cycle
      v
+----------------------------------------------------------------------+
|  Kafka  :9092   topic: clickstream                                   |
|  (Confluent Platform 7.5.0 — Kafka + Zookeeper)                     |
+----------------------------------------------------------------------+
                           | Spark Structured Streaming
                           v
+----------------------------------------------------------------------+
|  HDFS  hdfs://namenode:9000                                          |
|                                                                      |
|  /data/bronze   <- raw Parquet (Spark Streaming, append)            |
|  /data/silver   <- deduped + timestamps (Spark Batch, partition by  |
|                    event_date, overwrite)                            |
|  /data/gold     <- category aggregates (Spark Batch, partition by   |
|                    event_date, dynamic overwrite)                    |
+----------------------------------------------------------------------+
                           | Airflow DAG: clickstream_pipeline (@hourly)
                           | spark-submit: silver -> gold -> gold_to_postgres
                           v
+----------------------------------------------------------------------+
|  PostgreSQL   db: analytics   table: clickstream_metrics             |
|  (Gold data loaded via Spark JDBC — truncate + overwrite)            |
+----------------------------------------------------------------------+
                           | Airflow DAG: dbt_transform_pipeline (@hourly)
                           | dbt run -> dbt test -> dbt docs generate
                           v
+----------------------------------------------------------------------+
|  dbt (PostgreSQL adapter)                                            |
|  stg_clickstream (VIEW) -> fct_category_metrics + fct_daily_revenue |
+----------------------------------------------------------------------+
                           |
                           v
+----------------------------------------------------------------------+
|  Metabase  :3000   (connects to PostgreSQL analytics DB)             |
+----------------------------------------------------------------------+
```

---

## Stack

| Layer | Technology | Version |
|---|---|---|
| Event source | Fake Store API | — |
| Message broker | Apache Kafka + Zookeeper | Confluent 7.5.0 |
| Stream processing | Spark Structured Streaming | Spark 3.5.8 |
| Distributed storage | HDFS (Namenode + Datanode) | Hadoop 3.2.1 |
| Batch processing | PySpark (silver + gold jobs) | Spark 3.5.8 |
| Orchestration | Apache Airflow (LocalExecutor) | 2.x |
| Analytics DB | PostgreSQL | — |
| Transformation | dbt (PostgreSQL adapter) | — |
| Visualisation | Metabase | latest |
| Containerisation | Docker + Docker Compose (3 files) | — |
| Code quality | black, isort, flake8, pre-commit | — |

---

## Data layers (Medallion Architecture)

### Bronze — Raw events
**Location:** `hdfs://namenode:9000/data/bronze` | **Format:** Parquet (append)  
Written by Spark Structured Streaming directly from the Kafka topic.

| Field | Type | Description |
|---|---|---|
| `product_id` | INTEGER | Product ID from Fake Store API (1-20) |
| `category` | STRING | electronics, jewelery, men's clothing, women's clothing |
| `price` | DOUBLE | Product price in USD |
| `title` | STRING | Product title |
| `description` | STRING | Product description |
| `rating_rate` | DOUBLE | Average product rating |
| `rating_count` | INTEGER | Number of ratings |
| `event_ts` | LONG | Unix timestamp of the event |

### Silver — Cleaned & enriched
**Location:** `hdfs://namenode:9000/data/silver` | **Format:** Parquet, partitioned by `event_date`  
Spark batch job: casts epoch to timestamp, adds `event_date`, deduplicates by `(product_id, event_ts)`.

### Gold — Aggregated metrics
**Location:** `hdfs://namenode:9000/data/gold` | **Format:** Parquet, partitioned by `event_date`  
Spark batch job: groups by `(category, event_date)` to produce `total_events` and `total_revenue`.

| Field | Type | Description |
|---|---|---|
| `category` | STRING | Product category |
| `event_date` | DATE | Partition date |
| `total_events` | LONG | Count of events for that category + day |
| `total_revenue` | DOUBLE | Sum of prices for that category + day |

---

## Airflow DAGs

### `clickstream_pipeline` — @hourly
Orchestrates the Spark batch transformation chain:
```
silver_layer  ->  gold_layer  ->  gold_to_postgres
```
Each task is a `spark-submit` against `spark://spark-master:7077`.

### `dbt_transform_pipeline` — @hourly
Runs the dbt layer after Gold data lands in PostgreSQL:
```
dbt_run  ->  dbt_test  ->  dbt_docs_generate
```

---

## dbt Models

```
dbt_project/models/
├── sources.yml                     # declares raw.clickstream_metrics source
├── staging/
│   ├── _staging_models.yml         # schema tests
│   └── stg_clickstream.sql         # VIEW — cleans nulls, computes avg_price_per_event
└── marts/
    ├── _mart_models.yml            # schema tests
    ├── fct_category_metrics.sql    # TABLE — category metrics + revenue_pct_of_day
    │                               #         + revenue_rank (window function)
    └── fct_daily_revenue.sql       # TABLE — daily revenue rollup across categories

dbt_project/tests/
├── assert_no_future_events.sql     # custom test: event_date <= current_date
└── assert_positive_revenue.sql    # custom test: total_revenue >= 0
```

---

## Data Governance

```
data_governance/
├── data_contracts/
│   └── clickstream_contract.yml   # SLA, layer definitions, quality rules
├── schemas/
│   ├── bronze_schema.json         # Spark Streaming output schema
│   ├── silver_schema.json         # Post-deduplication schema
│   └── gold_schema.json           # Aggregated output schema
└── docs/
    └── data_dictionary.md         # Column definitions for all layers
```

Quality rules enforced via dbt tests:

- No null categories or event dates (silver, gold)
- No negative revenue (gold, dbt marts)
- No future event dates (gold, dbt marts)
- Price >= 0 (bronze, silver)

Validate contracts locally:
```bash
make governance-check
```

---

## Services & ports

| Container | Port | Description |
|---|---|---|
| `kafka` | `9092` | Kafka broker (external access) |
| `namenode` | `9870` | HDFS Namenode web UI |
| `spark-master` | `8080` / `7077` | Spark Master UI / submit endpoint |
| `airflow-webserver` | `8081` | Airflow UI |
| `metabase` | `3000` | Metabase dashboard |
| `producer` | — | Fake Store API -> Kafka |
| `spark-streaming` | — | Kafka -> Bronze (HDFS) |
| `dbt` | — | dbt run/test (docker profile: dbt) |

---

## Quickstart

**Prerequisites:** Docker + Docker Compose, Git

```bash
# 1. Clone
git clone https://github.com/reddy63/ecommerce-clickstream-pipeline.git
cd ecommerce-clickstream-pipeline

# 2. Configure
cp .env.example .env
# Edit .env — set POSTGRES_PASSWORD

# 3. Start infrastructure (Kafka, HDFS, Spark, Metabase, PostgreSQL)
docker compose -f docker-compose.infra.yml up -d

# 4. Start application layer (producer, Spark Streaming)
docker compose -f docker-compose.app.yml up -d --build

# 5. Start Airflow
docker compose -f docker-compose.airflow.yml up -d --build
```

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8081 | admin / admin |
| Spark Master | http://localhost:8080 | — |
| HDFS Namenode | http://localhost:9870 | — |
| Metabase | http://localhost:3000 | set on first login |

```bash
# 6. Trigger pipeline (or wait for @hourly Airflow schedule)
# Airflow UI -> DAGs -> clickstream_pipeline -> Trigger

# 7. Run dbt transformations manually
make dbt-run
make dbt-test
```

---

## Make commands

```bash
make up               # start infra + app layers
make down             # stop all services
make restart          # full rebuild
make logs             # tail app layer logs
make dbt-run          # run dbt models
make dbt-test         # run dbt tests
make dbt-docs         # generate + serve dbt docs
make lint             # black + isort + flake8 check
make format           # black + isort auto-format
make test             # pytest
make governance-check # validate JSON schemas + YAML contracts
make clean            # remove __pycache__, dbt target
```

---

## Project structure

```
ecommerce-clickstream-pipeline/
├── airflow/
│   ├── dags/
│   │   ├── clickstream_pipeline_dag.py   # silver -> gold -> postgres (@hourly)
│   │   └── dbt_transform_dag.py          # dbt run -> test -> docs (@hourly)
│   └── Dockerfile
├── data_governance/
│   ├── data_contracts/clickstream_contract.yml
│   ├── schemas/                          # bronze / silver / gold JSON schemas
│   └── docs/data_dictionary.md
├── dbt_project/
│   ├── models/
│   │   ├── staging/stg_clickstream.sql   # VIEW
│   │   └── marts/
│   │       ├── fct_category_metrics.sql  # TABLE (window functions)
│   │       └── fct_daily_revenue.sql     # TABLE
│   └── tests/                            # custom data quality tests
├── producer/
│   ├── producer.py                       # Fake Store API -> Kafka
│   └── Dockerfile
├── spark-jobs/
│   ├── silver_clickstream.py             # Bronze -> Silver (batch)
│   ├── gold_clickstream.py               # Silver -> Gold (batch)
│   └── gold_to_postgres.py              # Gold -> PostgreSQL (JDBC)
├── spark-streaming/
│   ├── clickstream_streaming.py          # Kafka -> Bronze (streaming)
│   └── Dockerfile
├── docker-compose.infra.yml              # Kafka, HDFS, Spark, Metabase
├── docker-compose.app.yml               # producer, spark-streaming, dbt
├── docker-compose.airflow.yml           # Airflow init + webserver + scheduler
├── .env.example
├── Makefile
└── .pre-commit-config.yaml
```

---

## Key concepts demonstrated

| Concept | Implementation |
|---|---|
| Medallion Architecture | Bronze / Silver / Gold layers on HDFS |
| Real-time streaming | Spark Structured Streaming <- Kafka topic |
| Batch processing | PySpark jobs orchestrated by Airflow |
| Distributed storage | HDFS (Hadoop 3.2.1) |
| DAG orchestration | Airflow with retry + @hourly schedule |
| SQL transformation | dbt models with window functions + custom tests |
| Data governance | Contracts, schemas, data dictionary, SLA definitions |
| JDBC integration | Spark -> PostgreSQL via org.postgresql driver |
| Code quality | pre-commit hooks (black, isort, flake8) |
| Containerised services | 3 Docker Compose files (infra / app / airflow) |

---

## License

MIT
