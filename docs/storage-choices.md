# Choosing the storage layer

> Short answer: **Parquet files in the lake for bronze, DuckDB for the silver +
> gold warehouse.** Move to Postgres when several processes need to write, and to
> ClickHouse / BigQuery / Snowflake when the data outgrows one machine.

This document explains that choice, because picking storage is the decision that
shapes everything else in a data platform.

---

## What the medallion architecture actually asks of storage

Each layer has genuinely different requirements, which is why one engine rarely
serves all three well:

| Layer | Access pattern | What storage must be good at |
|---|---|---|
| **Bronze** | Append-only, written once, read rarely, kept forever | Cheap bytes, schema-on-read, immutability |
| **Silver** | Write-heavy, many small merges, occasional full scans | Transactions, upserts, constraints |
| **Gold** | Read-heavy, aggregate scans, high concurrency | Columnar scans, fast group-by, wide tables |

## The options, honestly compared

| Option | Good at | Bad at | Use when |
|---|---|---|---|
| **Parquet files + DuckDB** *(this project)* | Zero ops, real columnar SQL, reads Parquet in place, `pip install` and go | One writer process at a time; single machine | Learning, prototypes, single-node analytics up to ~100s of GB |
| **PostgreSQL** | Multi-writer, mature, transactions, everyone knows it | Row-store: analytical scans get slow past tens of millions of rows | Several services write concurrently, or the warehouse also serves an app |
| **PostgreSQL + TimescaleDB / Citus** | Postgres semantics with columnar compression and partitioning | Extra operational surface | Time-series analytics where you want to stay on Postgres |
| **ClickHouse** | Extremely fast aggregate scans, built for log/event data | Weak on updates and joins; another server to run | Billions of log events, dashboards must stay sub-second |
| **Delta Lake / Apache Iceberg on object storage** | ACID on top of Parquet, time travel, schema evolution, engine-agnostic | Needs Spark/Trino/Flink or a catalog to be worth it | Multiple engines and teams share one lakehouse |
| **BigQuery / Snowflake / Redshift** | Elastic, no ops, scales past any single machine | Cost, vendor lock-in, needs cloud | Production at company scale |
| **SQLite** | Simplest possible | Row-store, no analytical performance | Never for a warehouse; fine for pipeline metadata |

## Why DuckDB wins *for this project*

1. **It is a real warehouse, not a toy.** Columnar storage, vectorised execution,
   window functions, `QUALIFY`, `GROUP BY ALL`, percentiles. The SQL you write
   here is the SQL you would write against Snowflake.
2. **It reads the lake directly.** `SELECT * FROM read_parquet('bronze/**/*.parquet')`
   means bronze needs no loading step at all — the lake *is* queryable. That is
   the lakehouse idea, available on a laptop.
3. **Nothing to operate.** No server, no credentials, no ports. The warehouse is
   one file you can copy, delete, or hand to a colleague.
4. **It fails in an instructive way.** Its single-writer limit forces you to
   design a proper writer/reader separation — which is exactly the discipline
   real warehouses require anyway. See below.

## The one limitation, and how this project handles it

**DuckDB permits one read-write process at a time.** A second process opening the
same file read-write gets `Could not set lock on file`.

That is not a bug to work around; it is a constraint to design against, and the
design is the same one large warehouses use:

```
        WRITERS                            READERS
   ┌──────────────────┐            ┌───────────────────────┐
   │ Airflow DAG tasks│            │ FastAPI  │ Streamlit  │
   └────────┬─────────┘            └──────────┬────────────┘
            │ exclusive file lock             │ in-memory DuckDB
            │ (src/pipeline/warehouse.py)     │ over Parquet
            ▼                                 ▼
   medallion.duckdb  ──── gold export ──►  lake/gold_exports/*.parquet
   (silver + gold tables)                  (the serving contract)
```

* **One writer role.** Only Airflow tasks write, and `writer_connection()` wraps
  every write in a cross-process `filelock`. Both DAGs also set
  `max_active_runs=1`.
* **Readers never touch the file.** The API and the dashboard query the gold
  Parquet exports through an in-memory DuckDB. No lock, unlimited concurrency,
  and a running pipeline can never take the dashboard down.
* **Publishing is atomic.** `gold.py` writes each export to `.tmp` and renames it,
  so a reader polling the directory can never see a half-written mart.

## When to graduate — concrete signals

| Signal | Move to |
|---|---|
| More than one process must write the warehouse | PostgreSQL |
| Silver exceeds what one machine can scan comfortably (~100s of GB) | ClickHouse, or Spark/Trino over Iceberg |
| You need time travel, schema evolution, or multi-engine access to the lake | Delta Lake or Apache Iceberg |
| Many concurrent dashboard users | ClickHouse, or a cloud warehouse |
| The team wants zero infrastructure | BigQuery / Snowflake |

The good news: because every transformation in this repo is plain SQL executed
through one module (`warehouse.py`), swapping the engine is a contained change.
That is the real reason to route all database access through a single module.

## Why *not* just use Postgres for everything?

You could, and for a small platform it is a perfectly defensible choice — it is
the "boring technology" answer and it multi-writes without ceremony. The trade is
performance shape: Postgres stores rows, so `SELECT service, count(*) ... GROUP BY`
over 50 million log events reads every column of every row. DuckDB reads only the
two columns the query names. On log analytics — wide rows, narrow queries, big
scans — that difference is one to two orders of magnitude.

Rule of thumb: **row store for the things you write and look up; column store for
the things you aggregate.** Silver and gold are aggregation targets, so they get
a column store.
