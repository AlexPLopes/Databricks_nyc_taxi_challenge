# NYC Yellow Taxi — Databricks Data Lake

Solution for ingesting NYC yellow taxi trip data (January–May 2023), building a two-layer data lake on Databricks (landing + consumption), exposing curated data via Unity Catalog, and answering the case analytical questions in SQL.

**Data source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

---

## Objective

1. Ingest original trip files into a **landing zone**
2. Clean and publish a **consumption layer** with only the required columns
3. Make data available to SQL consumers through a **catalog table**
4. Run the two analytical queries defined in the case

---

## Architecture

```text
TLC Parquet files (Jan–May 2023)
        │
        ▼
┌───────────────────────────────────────┐
│ 01_ingest_landing                     │
│  Validate / supply landing Volume     │
└───────────────────────────────────────┘
        │
        ▼
  /Volumes/.../landing/.../*.parquet   (raw)
        │
        ▼
┌───────────────────────────────────────┐
│ 02_landing_to_consumption             │
│  PySpark: QA, cleanse, cast schema     │
│  Write Delta → consumption Volume     │
└───────────────────────────────────────┘
        │
        ▼
  /Volumes/.../consumption/yellow_taxi   (Delta table on Volume)
        │
        ▼
┌───────────────────────────────────────┐
│ 03_register_table                     │
│  Read Delta → managed UC table         │
└───────────────────────────────────────┘
        │
        ▼
  nyc_taxi.yellow_trips_consumption      (SQL)
        │
        ▼
  analysis/queries.sql
```

| Layer | Storage | Catalog object | Notebook |
|-------|---------|----------------|----------|
| Landing | Unity Catalog Volume (`landing`) | — | `01_ingest_landing` |
| Consumption | Unity Catalog Volume (`consumption`) | — | `02_landing_to_consumption` |
| SQL serving | UC managed Delta (table storage) | `nyc_taxi.yellow_trips_consumption` | `03_register_table` |

---

## Repository structure

```text
Databricks_nyc_taxi_challenge/
├── src/
│   ├── 01_ingest_landing.ipynb
│   ├── 02_landing_to_consumption.ipynb
│   └── 03_register_table.ipynb
├── analysis/
│   └── queries.sql
├── README.md
└── requirements.txt
```

---

## Prerequisites

- Databricks workspace (tested on **Free Edition** with **Serverless** compute)
- Unity Catalog enabled (`workspace` or `main` catalog)
- Git repo connected in Databricks (**Repos**) optional but recommended

### Catalog objects (created in notebook 01)

| Object | Name |
|--------|------|
| Catalog | `workspace` (adjust if `SHOW CATALOGS` returns another name) |
| Schema | `nyc_taxi` |
| Volumes | `nyc_taxi.landing`, `nyc_taxi.consumption` |
| Table | `nyc_taxi.yellow_trips_consumption` (created in notebook 03) |

### Paths

| Layer | Path |
|-------|------|
| Landing | `/Volumes/workspace/nyc_taxi/landing/yellow_taxi/2023/{MM}/yellow_tripdata_2023-{MM}.parquet` |
| Consumption (Delta) | `/Volumes/workspace/nyc_taxi/consumption/yellow_taxi` |

Replace `workspace` with your catalog name if different.

---

## Data files (landing)

Download and upload **5 Parquet files** (yellow taxi, 2023-01 through 2023-05) to the landing Volume:

| Month | File |
|-------|------|
| Jan | `yellow_tripdata_2023-01.parquet` |
| Feb | `yellow_tripdata_2023-02.parquet` |
| Mar | `yellow_tripdata_2023-03.parquet` |
| Apr | `yellow_tripdata_2023-04.parquet` |
| May | `yellow_tripdata_2023-05.parquet` |

**Free Edition note:** Outbound access to TLC URLs is often blocked (`UnknownHostException`). Use **manual upload** to the landing Volume (UI: *Add or upload data → Upload files to a volume*).

---

## Execution order

Run notebooks on **Serverless** compute in this order:

1. `src/01_ingest_landing.ipynb`
2. `src/02_landing_to_consumption.ipynb`
3. `src/03_register_table.ipynb`
4. `analysis/queries.sql` (SQL Editor or `%sql` cell)

After code changes: **Run → Clear state** (or **Restart Python**) before re-running downstream notebooks.

---

## Notebook 01 — `01_ingest_landing`

**Purpose:** Landing supply and validation.

- Create schema and volumes (`landing`, `consumption`)
- Verify all 5 expected Parquet paths exist under the landing Volume
- Optional: row/column counts per month (`spark.read.parquet`)

**PySpark usage:** `spark.read.parquet` for validation reads.

**Does not** write to consumption or register tables.

---

## Notebook 02 — `02_landing_to_consumption`

**Purpose:** Data quality checks, cleansing, and Delta load to the consumption Volume.

### Read strategy

- Read **one file per month** (avoids Parquet schema conflicts across months)
- `unionByName` to build the full Jan–May dataset

### Cleansing rules (consumption contract)

| Rule | Implementation |
|------|----------------|
| Required columns only | `VendorID`, `passenger_count`, `total_amount`, `tpep_pickup_datetime`, `tpep_dropoff_datetime` |
| Non-null required fields | `filter(...isNotNull())` |
| Case period | Pickup in `[2023-01-01, 2023-06-01)` and `year(pickup) = 2023` |
| Valid amounts | `total_amount >= 0` |
| Valid passenger count | `0 <= passenger_count <= 9` |
| Valid trip duration | `dropoff_datetime > pickup_datetime` |
| Fixed schema | Explicit `.cast(...)` before write |

### Write to consumption

```python
df_treated.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(consumption_path)
```

Use `overwriteSchema` (or delete the consumption path) when the schema changes to avoid `DELTA_FAILED_TO_MERGE_FIELDS`.

**PySpark usage:** Main transformation notebook (`read`, `filter`, `agg`, `unionByName`, `write` Delta).

---

## Notebook 03 — `03_register_table`

**Purpose:** Publish the consumption Delta dataset as a Unity Catalog table for SQL consumers.

```python
spark.sql("DROP TABLE IF EXISTS nyc_taxi.yellow_trips_consumption")
df = spark.read.format("delta").load(consumption_path)
df.write.format("delta").mode("overwrite").saveAsTable("nyc_taxi.yellow_trips_consumption")
```

### Why not `CREATE TABLE ... LOCATION '/Volumes/...'`?

Unity Catalog external tables require a **cloud URI scheme** (`s3://`, `abfss://`, `gs://`). Paths under `/Volumes/...` cannot be used as `LOCATION` for external tables (`Missing cloud file system scheme`).

**Approach chosen:** Delta files on the consumption Volume (physical curated layer) + **managed table** via `saveAsTable` (SQL serving layer). This matches common enterprise patterns (curated files + catalog table), adapted for Free Edition constraints.

**PySpark usage:** `spark.read.format("delta")` and `saveAsTable`.

---

## Consumption schema

| Column | Type |
|--------|------|
| `VendorID` | `BIGINT` |
| `passenger_count` | `BIGINT` |
| `total_amount` | `DOUBLE` |
| `tpep_pickup_datetime` | `TIMESTAMP` |
| `tpep_dropoff_datetime` | `TIMESTAMP` |

---

## Analysis — `analysis/queries.sql`

Run against `nyc_taxi.yellow_trips_consumption` after notebook 03.

### Question 1 — Monthly average `total_amount` (Jan–May 2023)

Uses `date_format` for readable month labels (`2023-01`, …).  
`date_trunc('month', ...)` returns the first instant of each month (`2023-01-01T00:00:00Z`), which is correct for grouping but less readable in output.

### Question 2 — Average `passenger_count` by hour of day in May 2023

Filters May 2023 trips and groups by `hour(tpep_pickup_datetime)`.

See `analysis/queries.sql` for the full SQL.

---

## Technology choices

| Topic | Choice | Rationale |
|-------|--------|-----------|
| Compute | Serverless | Available on Databricks Free Edition |
| Raw storage | UC Volume `landing` | DBFS `/FileStore` disabled on Free Edition |
| Curated storage | Delta on Volume `consumption` | ACID, versioned curated layer |
| SQL access | Managed Delta table | Works without `s3://` external locations |
| Transform | PySpark (notebook 02) | Case requirement |
| Consumer queries | SQL | Case requirement |

### Enterprise (Databricks on AWS/Azure/GCP)

In production, consumption would typically live on **cloud storage** (`s3://` / `abfss://`) with an **external or managed Delta table** and `LOCATION` using the cloud scheme—not only UC Volumes. The three-notebook split (landing → transform → register) remains the same.

---

## Validation queries

```sql
-- Row count
SELECT COUNT(*) AS total_rows FROM nyc_taxi.yellow_trips_consumption;

-- Month distribution
SELECT date_format(tpep_pickup_datetime, 'yyyy-MM') AS trip_month, COUNT(*) AS trips
FROM nyc_taxi.yellow_trips_consumption
GROUP BY 1 ORDER BY 1;

-- Date range
SELECT MIN(tpep_pickup_datetime) AS min_pickup, MAX(tpep_pickup_datetime) AS max_pickup
FROM nyc_taxi.yellow_trips_consumption;
```

---

## Troubleshooting

| Issue | Likely cause | Mitigation |
|-------|--------------|------------|
| `DBFS_DISABLED` on `/FileStore` | Free Edition | Use UC Volumes under `/Volumes/...` |
| `UnknownHostException` on TLC URL | No outbound network | Manual upload to landing Volume |
| `PARQUET_COLUMN_DATA_TYPE_MISMATCH` | Schema differs across months | Read month-by-month; cast in notebook 02 |
| `DELTA_FAILED_TO_MERGE_FIELDS` | Old Delta/table schema | `overwriteSchema` or `dbutils.fs.rm(consumption_path, recurse=True)`; `DROP TABLE` before notebook 03 |
| `Missing cloud file system scheme` | `LOCATION '/Volumes/...'` | Use `saveAsTable` (notebook 03) |

---

## Author / delivery

- Repository: `Databricks_nyc_taxi_challenge`
- Case: Data Architect technical challenge
- Period: NYC yellow taxi, **January–May 2023**

