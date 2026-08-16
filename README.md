# NYC Taxi Data Lake — iFood Data Engineer Case

## 1. Title and overview

This repository implements the iFood Data Engineer case for NYC TLC Yellow Taxi trips. It downloads the official **January–May 2023** monthly Parquet files, preserves them in a replayable landing/Bronze path, profiles the untouched data, applies an explicit quality contract in Silver, publishes a daily-partitioned Gold Delta table, and answers the two business questions with PySpark.

The consumption contract keeps the five fields required by the case:

- `VendorID`
- `passenger_count`
- `total_amount`
- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`

The source is the [official NYC Taxi & Limousine Commission Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). The implementation downloads the monthly files from the TLC CloudFront URL template used in [`src/config.py`](src/config.py):

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet
```

The implementation deliberately does not put query-result numbers in this README. Results are generated from the verified Gold table at execution time, so the documentation does not turn a refreshable analytical output into an unversioned claim.

## 2. Architecture diagram

```text
NYC TLC CloudFront Parquet
            │
            ▼
Landing Zone (monthly immutable Parquet files)
            │
            ▼
Bronze — Delta: source columns + lineage, append-only
            │
            ▼
Silver — Delta: typed required columns + DQ boundary
            │
            ▼
Gold — Delta, partitioned by pickup_date (daily)
            │
            ├──────────────────────────────────────┐
            ▼                                      ▼
Q1: Monthly avg total_amount        Q2: May hourly avg passenger_count
```

The default production-oriented table names are three-level Unity Catalog identifiers:

```text
nyc_taxi.bronze.yellow_tripdata
nyc_taxi.silver.yellow_tripdata
nyc_taxi.gold.yellow_tripdata
```

For Databricks Community Edition, `PipelineConfig.community_edition()` uses the two-level Hive Metastore fallback `bronze.yellow_tripdata`, `silver.yellow_tripdata`, and `gold.yellow_tripdata`. This is a configuration difference, not a change to the logical model.

## 3. Why Medallion Architecture?

The case asks for both ingestion and a layer that consumers can query. A Bronze/Silver/Gold design makes the boundary between those responsibilities explicit instead of making one table serve incompatible purposes.

### Bronze — source fidelity and lineage

Bronze reads Parquet with the explicit 19-column `RAW_SCHEMA`, appends to Delta, and adds `_source_file`, `_ingestion_timestamp`, and `_ingestion_date`. It intentionally does not cleanse before EDA.

**Why:** keeping the source representation makes the pipeline replayable and auditable. If a Silver rule is wrong, a source file changes, or a downstream table is lost, the transformation can be rerun from Bronze without downloading the public files again. The source path and ingestion metadata also allow an operator to trace a record back to its input file and ingestion date. `mergeSchema=false` prevents a source-shape change from silently changing the Bronze contract.

### Silver — controlled quality boundary

Silver explicitly casts the five case fields (`VendorID` to `long`, numeric values to `double`, timestamps to `timestamp`), applies the named quality predicates, and projects only the five consumption columns.

**Why:** Silver is the appropriate place for quality decisions because it is downstream of a lossless raw copy but upstream of consumer-facing data. It prevents every consumer from independently interpreting nulls, invalid amounts, reversed timestamps, or out-of-scope dates. The `valid_rows()`/`invalid_rows()` functions make the predicates reusable and testable. The data contract calls the invalid path `quarantine`; the current case implementation excludes those rows from Silver and exposes the rejected rows through `invalid_rows()`. A production implementation should persist that result to a quarantine Delta table with the failed-rule names and lineage columns.

### Gold — stable consumer contract

Gold reads Silver, retains the required fields, derives a native `DateType` `pickup_date`, and writes a daily-partitioned Delta table.

**Why:** consumers should depend on a narrow, documented interface rather than the 19-column source schema. Gold can add a business-friendly derived column or change its physical layout without coupling SQL users to source changes. The daily partition is also an execution decision made for the actual questions, not an accidental consequence of the raw file layout.

### Why not one layer?

A single cleaned table would combine ingestion, quality policy, and consumption semantics:

- there would be no durable replay point after a cleansing mistake;
- source anomalies would be hidden before the exploratory analysis required by the evaluation criteria;
- quality rules and query-specific columns would be coupled to the raw source shape;
- a source schema change could break consumers directly; and
- rerunning a transformation could require re-downloading the source.

The medallion pattern follows the common Bronze/Silver/Gold organization used in iFood data products: preserve input first, enforce a controlled contract next, and expose stable consumer data last.

## 4. Why Delta Lake?

All persistent layers are written with `format("delta")`. Parquet remains the official input format, but Delta is the table format for managed, queryable layers.

| Delta capability | Technical reason for this project | How it appears in the implementation |
|---|---|---|
| **ACID transactions** | A table update is committed atomically. Readers do not observe a table half-written across multiple files, which is important for a registered consumption table. | Bronze append and Silver/Gold writes use Delta table writes. |
| **Time travel and audit** | Operators can inspect prior table versions and investigate when a schema or row count changed. | `DESCRIBE HISTORY <table>` is exposed by `describe_table_history()`. A reader can use `VERSION AS OF <version>` for a historical snapshot or rollback workflow. |
| **Schema enforcement** | A typo or unexpected source column should not silently become part of a stable contract. | Bronze writes set `mergeSchema=false`; Silver and Gold explicitly project their schemas. |
| **Controlled schema evolution** | Rebuilding a layer with an intentional contract change should update the table schema predictably rather than accumulate fields accidentally. | Silver and Gold use `overwriteSchema=true` together with explicit projections. This is controlled evolution, not permissive automatic merging. |
| **OPTIMIZE** | Partitioned or incremental writes can create small files. Compaction reduces file-open and task-scheduling overhead and gives readers fewer files to scan. | `optimize_table()` runs `OPTIMIZE <table>`. |
| **ZORDER BY** | Clustering related values improves Delta data skipping when a predicate is selective and the table is large enough to justify maintenance. | `zorder_table()` runs `OPTIMIZE <table> ZORDER BY (...)`; the case run uses `VendorID`. |
| **VACUUM** | Obsolete files remain useful for time travel but consume storage. Removing files older than the retention policy controls cost. | `vacuum_table()` enforces a 168-hour minimum and `run_all_optimizations()` invokes it. |
| **MERGE** | Corrections and idempotent keyed updates can be applied atomically without replacing an entire table. | MERGE is a production extension for corrections; this bounded case uses deterministic Silver/Gold overwrite and keeps Bronze append-only. |

### Operational interpretation

- **ACID is not a substitute for idempotency.** The downloader still skips complete local files, writes through a temporary file, and uses `os.replace()` so Spark never sees a partial landing file. Delta makes the table commit reliable after the input is ready; it does not make a broken download valid.
- **Time travel has a retention dependency.** `DESCRIBE HISTORY` records transaction versions, but `VACUUM` eventually removes old physical files. Therefore the retention window must be agreed with audit and recovery needs before running a shorter vacuum. This project refuses less than seven days (`168` hours).
- **Schema enforcement and evolution have different jobs.** `mergeSchema=false` protects Bronze from accidental drift. `overwriteSchema=true` is used only after the code has explicitly projected the intended Silver/Gold contract.
- **OPTIMIZE is maintenance, not a transformation.** It should be scheduled after ingestion or when file counts justify its cost, not blindly inserted into every query.
- **Z-ordering is not partitioning.** `pickup_date` physically partitions the table. Z-ordering is a secondary clustering/data-skipping technique and is useful only when the filtered column and table scale make the rewrite cost worthwhile.
- **MERGE requires a real key.** The case does not define a unique trip identifier. A production MERGE must use a documented business key or source-file/row identity; inventing a key from non-unique business fields could duplicate or overwrite trips.

### Contrast with plain Parquet

Parquet is columnar and useful as an interchange format, but plain Parquet alone does not provide a transaction log with atomic table commits, built-in table-version time travel, schema enforcement at the table boundary, or Delta maintenance commands such as `OPTIMIZE` and `VACUUM`. Those properties justify converting the raw files into Delta at Bronze rather than keeping every layer as unmanaged Parquet.

## 5. Partitioning strategy

Gold is partitioned by the native `DateType` column `pickup_date`, derived directly from `tpep_pickup_datetime`:

```python
gold_df.write.format("delta") \
    .partitionBy("pickup_date") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_table)
```

### Why daily partitioning?

1. **It matches both query shapes.** Q1 groups by pickup month and Q2 filters a precise month before grouping by hour. A pickup date is the natural physical key for temporal access.
2. **It enables partition pruning for Q2.** May 2023 contains 31 calendar days. Against a complete January–May range of 151 days, the Gold scan can read only the 31 May partitions and skip roughly 120 non-May partitions. The query also retains timestamp bounds to preserve the exact semantic interval and enable data skipping/predicate pushdown.
3. **Cardinality is bounded.** Approximately 151 daily partitions for this five-month case is moderate: granular enough to prune a month, but not one partition per trip or another high-cardinality key that would create many tiny files.
4. **The key is a real date, not a formatted string.** Native `DateType` supports correct comparisons, date functions, and partition pruning without reparsing a presentation key.

### Trade-offs and rejected alternatives

| Alternative | Why it was not selected |
|---|---|
| **Monthly partitioning** | It would create only five partitions and would avoid excessive cardinality, but a May query would still scan one broad May partition. It is coarser than necessary for a daily-partitioned temporal workload. Monthly partitioning could be reasonable for a much larger history where daily files are too small, but it is not the best fit for this bounded case. |
| **Partition by `VendorID`** | The EDA is expected to show only a small vendor domain—typically `1` and `2`, with occasional `6` depending on source coverage. Two or three partitions are too few and can be unbalanced; this key does not help either time-based question prune by date. `VendorID` is instead a candidate for Z-ordering when vendor-filtered queries justify it. |
| **No partitioning** | Every May query would start from the full Gold file set. Delta statistics and predicate pushdown could still skip some data, but there would be no directory-level partition pruning. |
| **Hourly partitioning** | It is unnecessarily granular for the case and risks small files while adding no business requirement beyond a grouping expression. Extracting the hour in the query is cheaper and more flexible. |

The right partition key is workload-dependent. If the table later covers years of data or receives very small daily increments, the production team should reassess file sizes, partition counts, and an alternate strategy rather than treating “daily” as universal.

## 6. Data quality strategy

The quality functions return predicates that are **true for invalid rows**. `valid_rows()` negates the combined OR expression, so a row must pass every expectation to enter Silver. The same registry is intentionally shaped like a DLT/SDP expectation registry: `get_data_quality_expectations()` maps names to predicates that can be wired to `@dlt.expect_or_drop` in a DLT implementation.

| Rule | SQL invalid-row logic | Why it matters |
|---|---|---|
| `required_columns_not_null` | `VendorID IS NULL OR passenger_count IS NULL OR total_amount IS NULL OR tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL` | A missing required field cannot support the case contract or a trustworthy aggregate. It is better to reject/quarantine the row than let each consumer choose a different null policy. |
| `passenger_count_positive` | `passenger_count IS NULL OR passenger_count <= 0` | Zero or negative passenger counts are not usable as a trip passenger measure and are data errors for this analysis. The EDA explicitly counts zero/non-positive values before applying the rule. |
| `total_amount_non_negative` | `total_amount IS NULL OR total_amount < 0` | A negative total is inconsistent with the requested “value received” measure. It may represent a refund or correction, but without a refund business contract it should not silently enter this case aggregate. |
| `dropoff_after_pickup` | `tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL OR tpep_dropoff_datetime <= tpep_pickup_datetime` | Equal or reversed timestamps indicate a corrupt or unusable trip interval. The rule avoids deriving duration or treating an impossible event sequence as valid. |
| `pickup_in_case_period` | `tpep_pickup_datetime IS NULL OR tpep_pickup_datetime < TIMESTAMP '2023-01-01' OR tpep_pickup_datetime >= TIMESTAMP '2023-06-01'` | The requested scope is January through May 2023. The half-open interval avoids ambiguity at the June boundary and protects the Gold contract if files contain late, early, or otherwise out-of-scope records. |

### Why apply DQ in Silver?

Bronze remains lossless for investigation and replay. Silver is where the five-field consumer contract is enforced, so quality logic is centralized and applied before Gold or analytical SQL. `notebooks/02_eda.py` runs after Bronze ingestion specifically so the rules are observable decisions rather than hidden assumptions. The current case path rejects invalid rows from Silver; a production DLT pipeline could use the same functions with `@dlt.expect_or_drop`, and a production batch path should additionally write a quarantine table containing the original fields, lineage, and failed expectations.

## 7. Exploratory data analysis — findings and decisions

The EDA notebook is intentionally a first-class pipeline step, before Silver filtering. It inspects schema and nullability, monthly volume, vendor frequency, null rates, descriptive statistics and percentiles, passenger distribution, amount tails, temporal coverage, reversed timestamps, explicit DQ issue counts, vendor-to-amount relationships, and daily trip volumes. It displays evidence rather than asserting hard-coded counts, because TLC files can be refreshed.

### Findings the notebook is designed to expose

- **VendorID distribution:** the source is generally concentrated in `VendorID` 1 and 2, with occasional `VendorID` 6 in some source coverage. The notebook groups and counts every observed vendor and compares vendor-level mean/median amounts. This supports retaining `VendorID` as a required field while rejecting it as a partition key.
- **Passenger counts:** most trips are expected to have one or two passengers; the notebook displays the full distribution, counts zero/non-positive values, and surfaces high counts above eight. Zeroes are not silently converted to one: they are visible in EDA and rejected by `passenger_count_positive` for this contract. Large values remain an observable tail for review rather than an invented cutoff.
- **Total amounts:** amounts are generally positive, while the EDA explicitly counts negatives and values above `$1,000`, and reports approximate quartiles through the 99th percentile. Negative values are rejected because the case asks for value received and no refund semantics were supplied. Large positive values are surfaced as outliers rather than removed by an arbitrary upper bound.
- **Temporal range:** the notebook reports minimum and maximum pickup/dropoff timestamps and counts pickups outside the January–May 2023 interval. This matters because the input files can contain records that need to be excluded from the case scope; `pickup_in_case_period` implements the half-open `[2023-01-01, 2023-06-01)` rule.
- **Data quality issues:** null required fields, zero passengers, negative amounts, and `dropoff <= pickup` are each counted. These are not assumptions inferred from a clean sample; they are explicit checks in the notebook and correspond to named Silver rules.
- **Daily volume:** trips per `pickup_date` and min/max/average daily counts are displayed. This is the operational evidence for choosing a bounded daily partition and for detecting gaps or unexpectedly small partitions.

The README intentionally does not state exact row counts, percentages, or percentile values because the repository does not contain a checked-in source snapshot and the notebook computes them at runtime. On Databricks, the displayed tables are the authoritative EDA result for that run.

### How EDA informs the transformation

The chain is:

1. The source schema and null-rate table establish which fields are safe to project and which required fields need explicit validation.
2. Passenger, amount, and timestamp anomaly counts establish the five named invalid predicates.
3. The temporal range confirms that a case-period rule is necessary instead of assuming the monthly files contain only in-scope records.
4. Vendor and daily-volume distributions show that time is the useful access dimension and `VendorID` is better treated as an analytical attribute or Z-order candidate.
5. Only after these observations does Silver reduce the schema and Gold choose the daily `pickup_date` layout.

This sequencing directly addresses the case evaluation criterion for **processo de análise exploratória**: the analysis is evidence for modeling and DQ, not an afterthought after the answers are calculated.

## 8. Query optimization

The standard and optimized analysis functions return the same business result contract. The optimized variants make execution choices visible so the physical plan can be inspected with `explain(mode="formatted")`.

### Predicate pushdown and partition pruning

Q2 applies both filters immediately after reading Gold:

```python
(F.col("pickup_date") >= F.to_date(F.lit("2023-05-01"))) &
(F.col("pickup_date") < F.to_date(F.lit("2023-06-01"))) &
(F.col("tpep_pickup_datetime") >= F.lit("2023-05-01")) &
(F.col("tpep_pickup_datetime") < F.lit("2023-06-01"))
```

- The `pickup_date` predicate matches the physical partition column, so Delta/Spark can prune non-May directories before reading files.
- The timestamp predicate preserves the exact business interval and can be pushed into the scan/data-skipping layer.
- Extracting `hour` happens after filtering, which avoids computing an hour for out-of-scope rows.

For the complete five-month case, Q2 is designed to read the 31 May partitions and skip approximately 120 other daily partitions. The exact physical plan and file counts must be verified in the Databricks run, because file layout and optimizer behavior depend on the runtime and table statistics.

### Broadcast join

The optimized Q2 creates a three-row inline `VendorID` lookup and uses `F.broadcast(vendor_lookup)`. This is a deliberate demonstration of the DataFrame equivalent of `/*+ BROADCAST(vendor_lookup) */`: a genuinely tiny dimension is copied to executors, avoiding a shuffle of the large fact side. The left join preserves the all-vendor trip population and the lookup is not needed to calculate the requested averages.

**Use it when:** the dimension is demonstrably small, stable, and fits comfortably in executor memory. **Do not use it when:** the dimension can grow beyond the broadcast threshold, has high cardinality, or would create executor memory pressure. For a real vendor/zone dimension, inspect size and plan metrics rather than forcing a broadcast by habit.

### Adaptive Query Execution (AQE)

The analysis sets `spark.sql.adaptive.enabled = true`. AQE lets Spark use runtime statistics to coalesce shuffle partitions and adapt physical decisions after stages complete. This is useful because May is a filtered subset of the five-month fact table and its actual volume may differ from a static estimate.

**Use it when:** workloads have filters, skew, or uncertain runtime cardinality and the Spark version supports the feature. **Do not treat it as magic:** it cannot replace correct partitioning, a selective predicate, or an appropriate join strategy; inspect the executed plan and metrics.

### Other explicit choices

- Q1 uses a distributed aggregation and orders only the small monthly result, not the full trip table. The optimized variant documents a 10 MiB broadcast threshold and uses a `REPARTITION(8)` hint for predictable parallelism in this case-sized workload.
- Q2 filters before joining, so the lookup is applied after the largest possible reduction.
- `ZORDER BY VendorID` is a maintenance option for selective vendor filters and file-level skipping. It does not replace `pickup_date` partition pruning and may not improve the two required queries materially because there are only a few vendor values.
- **Do not optimize blindly:** `OPTIMIZE`, Z-ordering, repartitioning, and broadcast joins add work or memory pressure. In production, compare scan bytes, files read, shuffle size, task skew, and wall-clock time before and after each change.

## 9. iFood pattern alignment

- **DAB/SDP-style modularity:** Databricks notebooks are thin orchestration entry points. Reusable ingestion, schemas, DQ, transformations, Delta maintenance, and analysis logic live in `src/` and `analysis/`. This keeps logic testable and avoids putting the only implementation inside a notebook.
- **Unity Catalog:** default identifiers use `catalog.schema.table`, such as `nyc_taxi.gold.yellow_tripdata`. The config also makes the Community Edition two-level fallback explicit, so portability is deliberate rather than hidden.
- **DLT expectations:** `get_data_quality_expectations()` returns named invalid predicates that map to `@dlt.expect_or_drop` or equivalent SDP expectations. The current implementation uses native DataFrame filtering; migrating to DLT would preserve the rule registry and add expectation metrics/managed pipeline behavior.
- **Data contracts:** [`data_contracts/yellow_tripdata_contract.yaml`](data_contracts/yellow_tripdata_contract.yaml) declares source coverage, table names, schema, required fields, quality expressions, partition rationale, SLA, ownership, and lineage. The contract is versioned alongside code so a consumer can review the interface before deployment.
- **Medallion:** Bronze preserves source and lineage, Silver enforces quality and typing, and Gold provides the stable query contract.
- **Delta Lake:** Delta is the native table format selected for ACID commits, schema controls, transaction history/time travel, and maintenance operations.
- **Deployment with DAB:** in a production workspace, package the notebooks, Python modules, config, and tests as a Databricks Asset Bundle and deploy the target environment with `databricks bundle deploy -t dev`.
- **Production orchestration:** Airflow can orchestrate the DAB-deployed workflow, including source-availability checks, the ingestion job, DQ/quarantine monitoring, Gold maintenance, and downstream publication. The monthly refresh SLA in the contract is a business/operational policy, not a claim that the case has already been scheduled.
- **Native Spark APIs:** transformations use `pyspark.sql.functions` rather than UDFs or RDDs, preserving Catalyst optimization and distributed execution.

## 10. Repository structure

```text
data/ifood-case/
├── .gitignore                              # Ignores Python caches, Spark/Databricks state, and generated data.
├── README.md                               # This technical design, rationale, and execution guide.
├── requirements.txt                         # PySpark, Delta, YAML, requests, and pytest dependencies.
├── data_contracts/
│   └── yellow_tripdata_contract.yaml        # Versioned schema, DQ, ownership, SLA, lineage, and storage contract.
├── src/
│   ├── __init__.py                          # Package marker and module description.
│   ├── config.py                             # Immutable runtime config, table names, dates, and source URL builder.
│   ├── schemas.py                            # Explicit 19-column raw and five-column consumption/Gold schemas.
│   ├── ingestion.py                          # Idempotent streamed download with atomic landing-file publication.
│   ├── bronze.py                             # Schema-checked append-only Bronze Delta ingestion with lineage.
│   ├── data_quality.py                       # Five named invalid-row predicates and schema validation.
│   ├── silver.py                             # Typed Silver projection, DQ filtering, and persisted row summaries.
│   ├── gold.py                               # Daily-partitioned Gold model and partition observability summary.
│   ├── delta_optimizations.py                # OPTIMIZE, ZORDER, VACUUM, history, and maintenance orchestration.
│   └── transformations.py                    # Pure date, month, and hour derivation helpers for deterministic tests.
├── analysis/
│   ├── __init__.py                           # Package marker for reusable analytical functions.
│   ├── q1_monthly_avg_amount.py              # Q1 standard and optimized monthly amount aggregations.
│   └── q2_avg_passengers_per_hour.py         # Q2 standard and optimized May hourly aggregations.
├── notebooks/
│   ├── 00_run_all.py                         # One-command end-to-end Databricks orchestration and final outputs.
│   ├── 01_ingestion_bronze.py      # Download and Bronze append step.
│   ├── 02_eda.py                 # Pre-transformation schema, distribution, anomaly, and volume profiling.
│   ├── 03_silver.py                          # Silver transformation and DQ summary step.
│   ├── 04_gold.py                            # Gold modeling, partition inspection, and Delta maintenance step.
│   └── 05_analysis.py                        # Standalone Q1/Q2 execution and physical-plan inspection.
└── tests/
    ├── __init__.py                           # Test package marker.
    ├── test_schemas.py                       # Raw, consumption, and Gold schema tests.
    ├── test_data_quality.py                  # Exact schema comparison and five expectation registry tests.
    └── test_transformations.py               # Date, month, and hour derivation tests.
```

The tree excludes ignored runtime artifacts such as `__pycache__`, `.pytest_cache`, Spark warehouse files, and downloaded Parquet files. They are execution state, not source deliverables.

## 11. Execution instructions

### Databricks Community Edition — recommended

1. Create a **single-node cluster** using **Runtime 15.x LTS or newer/compatible** with Spark 3.5 and Delta support.
2. Import this directory as a Databricks Repo or upload the files. The notebook expects the repository root to be available for `src` and `analysis` imports.
3. Open `notebooks/00_run_all.py` and choose **Run All**.
4. The one command performs cleanup, download, Bronze ingestion, EDA, Silver, Gold, Delta optimizations, and Q1/Q2 analysis. It prints row-count summaries, displays EDA tables, displays both query variants, and ends with `PIPELINE STATUS: SUCCESS` when all steps complete.
5. The notebook sets `USE_COMMUNITY_EDITION = True`, uses two-level table names, and defaults the landing path to `/dbfs/FileStore/nyc_taxi/landing`. Change that path if the workspace uses a different writable location.
6. If the cluster already supplies compatible Spark/Delta libraries, do not install a conflicting second Spark runtime; use `requirements.txt` as the dependency reference and install only what the workspace does not provide.

The all-in-one notebook intentionally drops the configured tables and clears its landing directory first. This makes a case rerun deterministic, but it is not a production incremental-reset policy. Production should use a run/batch identifier and controlled overwrite or MERGE semantics.

### Step-by-step alternative

Run the notebooks in this order when inspecting each boundary independently:

```text
01_ingestion_bronze.py → 02_eda.py → 03_silver.py → 04_gold.py → 05_analysis.py
```

Set `USE_COMMUNITY_EDITION = True` in the standalone notebooks when using two-level Hive Metastore names. The standalone EDA assumes Bronze already exists; the later notebooks likewise assume their upstream table exists.

### Local validation

From the project root:

```bash
PYTHONPATH=. pytest -q
```

The tests are intentionally split between pure helpers and schema/contract checks. A real Spark/Delta runtime is required to execute the Databricks pipeline itself; local unit tests do not download the five public files or create a Delta table.

### Production deployment at iFood

- Use the default Unity Catalog configuration or provide environment-specific catalog/schema/table names through `PipelineConfig`.
- Package and deploy with Databricks Asset Bundles:

  ```bash
  databricks bundle deploy -t dev
  ```

- Orchestrate the deployed jobs with Airflow + DAB, including source-file completeness, DQ rejection/quarantine monitoring, Delta maintenance, and downstream notification.
- Before production registration, replace governance tag examples with confirmed organizational values and define the business key/retention policy needed for MERGE and VACUUM.

## 12. Analytical questions

### Q1 — average `total_amount` per month

Implementation: [`analysis/q1_monthly_avg_amount.py`](analysis/q1_monthly_avg_amount.py).

```python
spark.read.table(gold_table) \
    .withColumn("pickup_month", F.date_format("tpep_pickup_datetime", "yyyy-MM")) \
    .groupBy("pickup_month") \
    .agg(
        F.avg("total_amount").alias("average_total_amount"),
        F.count(F.lit(1)).alias("trip_count"),
    ) \
    .orderBy("pickup_month")
```

The average is computed over valid Gold trips grouped by the pickup month. `trip_count` is included for transparency: a consumer can interpret an average together with the population supporting it. The query does not treat negative amounts as valid because Silver has already applied the case contract.

### Q2 — average `passenger_count` per pickup hour in May 2023

Implementation: [`analysis/q2_avg_passengers_per_hour.py`](analysis/q2_avg_passengers_per_hour.py).

```python
spark.read.table(gold_table) \
    .filter(
        (F.col("pickup_date") >= F.to_date(F.lit("2023-05-01"))) &
        (F.col("pickup_date") < F.to_date(F.lit("2023-06-01"))) &
        (F.col("tpep_pickup_datetime") >= F.lit("2023-05-01")) &
        (F.col("tpep_pickup_datetime") < F.lit("2023-06-01"))
    ) \
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime")) \
    .groupBy("pickup_hour") \
    .agg(
        F.avg("passenger_count").alias("average_passenger_count"),
        F.count(F.lit(1)).alias("trip_count"),
    ) \
    .orderBy("pickup_hour")
```

The `pickup_date` bounds are the physical partition-pruning predicate. The timestamp bounds are retained as the exact semantic filter. Consequently, the query is both correct at the boundary and efficient for the daily Gold layout.

## 13. Mandatory tags — iFood governance

The following checklist documents tags that production Unity Catalog objects should carry. The code does **not** apply guessed organization metadata automatically; the owning team must confirm the real layer, domain, owners, and service values before registration.

| Tag name | Purpose | Example value |
|---|---|---|
| `owner-layer-slug` | Owning organizational layer | `data-platform` |
| `data-domain-layer-slug` | Domain ownership | `mobility` |
| `data_classification` | Sensitivity classification | `public` |
| `owners` | Responsible group or users | `data-platform@example.invalid` *(placeholder)* |
| `service-name` | Producing service | `nyc-taxi-data-lake` |
| `layer` | Medallion layer | `bronze`, `silver`, or `gold` |

Apply the `layer` value separately to each table. Confirm whether the organization’s current governance vocabulary uses any renamed key before deployment; the YAML contract records ownership metadata but does not prove that Unity Catalog tags have been applied.

## 14. Testing

### Test coverage

| Test file | Coverage |
|---|---|
| `tests/test_schemas.py` | Covers raw field count, exact field order, key Spark data types, nullability, required-column subset alignment, consumption `LongType`/non-nullability, and the six-field non-nullable Gold schema. |
| `tests/test_data_quality.py` | Covers exact schema acceptance/rejection for wrong types, extra/missing/reordered fields, case-period constants, expectation names/callability, and mocked predicate composition for invalid and valid rows. |
| `tests/test_transformations.py` | Covers datetime/date/ISO-8601 normalization, UTC-suffixed timestamps, midnight and month boundaries, hour extraction, and unsupported input types. |
| `tests/test_config.py` | Covers URL formatting and validation, immutable default/custom pipeline configuration, month/year/partition validation, and Community Edition table fallbacks. |
| `tests/test_ingestion.py` | Covers directory creation, idempotent existing-file skips, returned paths, HTTP/empty-body failures, atomic temporary-file replacement, cleanup, and multi-month downloads with mocked requests. |
| `tests/test_bronze.py` | Covers Bronze validation for empty file lists and empty or whitespace-only table names before Spark access. |
| `tests/test_silver.py` | Covers Silver and summary table-name validation plus the overwrite write-mode contract. |
| `tests/test_gold.py` | Covers Gold and summary table-name validation plus daily partition and overwrite constants. |
| `tests/test_delta_optimizations.py` | Covers validation-before-Spark behavior for OPTIMIZE, ZORDER, VACUUM, and history, retention limits, and generated OPTIMIZE SQL. |
| `tests/test_analysis.py` | Covers Q1/Q2 table-name validation, May/June boundary constants, and mocked construction of the May filter without a Spark session. |
| `tests/test_data_contract.py` | Covers YAML version/status/storage, source and consumption schemas, quality-rule names/count, partitioning, Unity Catalog table names, and Community Edition fallbacks. |

### Run the tests

```bash
cd data/ifood-case
PYTHONPATH=. pytest -q
```

The test suite validates the contracts and pure transformation rules without requiring a live download. Before considering a Databricks run complete, also verify the runtime evidence produced by the notebooks:

- all five monthly landing files are present and non-empty;
- Bronze has a non-zero row count and the expected 19-column schema;
- EDA tables show the observed temporal range and anomaly counts;
- Silver has the five-column consumption schema and a visible rejected-row count;
- Gold is partitioned by `pickup_date` and has the expected in-scope date range;
- `DESCRIBE HISTORY` returns Delta transaction records; and
- standard and optimized Q1/Q2 results agree, while Q2’s formatted physical plan shows the May filters and, in supported runtimes, the broadcast join/AQE plan.

These checks distinguish “the Python files compiled” from “the data product actually published the intended contract.”
