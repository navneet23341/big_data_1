# A1_2023341 — Big Data Analytics Assignment 1

MapReduce vs Apache Spark vs DuckDB on NYC Yellow Taxi (January 2026).

Repository (code only, no raw data): https://github.com/navneet23341/big_data_1

## 1. Machine configuration (repeated in every row of `results/timings.csv`)

| Field | Value |
|---|---|
| OS | Linux Mint 22.3 (Live USB session) |
| CPU | 11th Gen Intel(R) Core(TM) i3-1115G4, 3.00 GHz base |
| Physical cores / logical processors | 2 / 4 |
| RAM | 8 GB |
| Storage | 64 GB, type recorded as "Other (Live USB)" |
| GPU | Integrated / none used |

**Non-default note:** all timed experiments were run from a Live USB boot, not an installed OS on internal storage. Live USB storage is typically slower than a normal NVMe/SATA SSD, and 8GB RAM is on the low end for running Spark + DuckDB + Python side by side. This is called out explicitly in `report/observations.pdf` as context for interpreting the absolute timings — relative comparisons between the three systems should still hold, but absolute numbers (especially MapReduce's per-run overhead) may be inflated compared to a typical installed laptop.

## 2. Environment setup

```bash
# clone the project directly into the required submission folder name
git clone https://github.com/navneet23341/big_data_1.git A1_2023341
cd A1_2023341
# (or: unzip A1_2023341.zip && cd A1_2023341)

# create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# install exact dependencies
python -m pip install mrjob pyspark duckdb pandas pyarrow matplotlib pypdf reportlab
```

**Exact versions used for this submission** (confirm on your own machine with the commands below — versions may differ slightly and that is allowed per the assignment, as long as they are reported):

```bash
python --version
pip show mrjob pyspark duckdb | grep -E "Name|Version"
```

Recorded for this submission:
- Python: `3.12.3`
- mrjob: `0.7.4`
- pyspark: *(see `results/timings.csv` — tool_version column)*
- duckdb: *(see `results/timings.csv` — tool_version column)*

## 3. Dataset setup — do this first

This repo does **not** include the raw taxi data (per assignment rules). You must download it yourself and place it in a local `data/` folder (NOT included in the submission ZIP):

```bash
mkdir -p data
cd data

# Download the January 2026 Yellow Taxi Parquet file from the official TLC page
# (place/rename it exactly as below)
#   -> data/yellow_tripdata_2026-01.parquet

# Download the taxi zone lookup table
curl -O https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
# -> data/taxi_zone_lookup.csv

cd ..
```

Generate the four fixed-size samples (100K / 500K / 1M / 2M rows, both CSV and Parquet) using DuckDB:

```bash
cd data
duckdb < ../data_prep/prepare_data.sql
cd ..
```

**Verify row counts before running any timed task** (required by the assignment spec):

```bash
for f in data/taxi_*.csv; do echo "$f: $(($(wc -l < "$f") - 1)) rows"; done
```

Expected output: `taxi_100k.csv: 100000 rows`, `taxi_500k.csv: 500000 rows`, `taxi_1m.csv: 1000000 rows`, `taxi_2m.csv: 2000000 rows`.

Expected files in `data/` before running anything else:
```
data/yellow_tripdata_2026-01.parquet
data/taxi_zone_lookup.csv
data/taxi_100k.csv       data/taxi_100k.parquet
data/taxi_500k.csv       data/taxi_500k.parquet
data/taxi_1m.csv         data/taxi_1m.parquet
data/taxi_2m.csv         data/taxi_2m.parquet
```

## 4. Running each task

All commands below are run from the project root (`A1_2023341/`), with the virtual environment activated. These are the exact commands used to produce the submitted results.

### Task 1 — one aggregation, three systems (`taxi_1m.csv`)

```bash
# MapReduce (1 warm-up + 3 recorded runs)
for i in 1 2 3 4; do
  echo "Run $i:"
  time python mapreduce/task1.py -r local data/taxi_1m.csv > results/task1_mapreduce_run$i.txt
done

# Spark (script handles warm-up + 3 runs internally)
python spark/task1.py --input data/taxi_1m.csv

# DuckDB (script handles warm-up + 3 runs internally)
python duckdb/task1.py --input data/taxi_1m.csv
```

### Task 2 — Parquet filtering and projection (`taxi_2m.parquet`)

```bash
python spark/task2.py --input data/taxi_2m.parquet
python duckdb/task2.py --input data/taxi_2m.parquet
```

Outputs saved automatically: `spark/task2_explain.txt`, `duckdb/task2_explain.txt`.

### Task 3 — join with taxi-zone lookup (`taxi_2m.parquet` + `taxi_zone_lookup.csv`)

```bash
python spark/task3.py --trips data/taxi_2m.parquet --zones data/taxi_zone_lookup.csv
python duckdb/task3.py --trips data/taxi_2m.parquet --zones data/taxi_zone_lookup.csv
```

Output saved automatically: `spark/task3_explain.txt`.

### Task 4 — scaling experiment (100K / 500K / 1M / 2M rows, CSV)

```bash
python mapreduce/task4.py
python spark/task4.py
python duckdb/task4.py

# build the required scaling plot from the three raw CSVs above
python results/make_scaling_plot.py
```

Outputs: `results/task4_mapreduce_raw.csv`, `results/task4_spark_raw.csv`, `results/task4_duckdb_raw.csv`, `results/scaling_plot.png`.

### Task 5 — repeated work on the same data (`taxi_1m.parquet`)

```bash
python spark/task5.py
python duckdb/task5.py
```

Outputs: `results/task5_spark_raw.csv`, `results/task5_duckdb_raw.csv`.

### Merging everything into the required `results/timings.csv`

```bash
python results/merge_timings.py
```

Before running this, open `results/merge_timings.py` and fill in the `STUDENT_ID`, `MACHINE_CONFIG`, and `TOOL_VERSIONS` fields at the top with your own machine's values (see Section 1 and 2 above for how to obtain them).

## 5. Verifying results agree across systems

Task 1, 2, and 3 numerical outputs were spot-checked across all systems to confirm agreement up to floating-point rounding, e.g.:

```bash
grep '"PULocationID": 148' results/task1_mapreduce_run4.txt
python -c "
import duckdb
print(duckdb.sql(\"SELECT PULocationID, COUNT(*) trip_count, AVG(fare_amount) avg_fare FROM read_csv_auto('data/taxi_1m.csv', header=true) WHERE PULocationID=148 AND fare_amount IS NOT NULL AND fare_amount>=0 GROUP BY PULocationID\").fetchall())
"
```

## 6. Known non-default conditions affecting reproducibility

- **Live USB environment** — see Section 1. Absolute timings (particularly MapReduce's ~3s fixed overhead per run) are likely higher than on a normally installed OS on internal SSD/NVMe storage.
- **DuckDB Task 1 uses `read_csv_auto`**, which performs CSV schema auto-detection on every call, while Spark's Task 1 script reads with an explicit schema (`header=True, inferSchema=False` + manual casts). This is a deliberate difference in query implementation, not a bug — it is discussed in `report/observations.pdf` (§5, "a prediction that was wrong") as the likely explanation for Spark outperforming DuckDB on Task 1 despite DuckDB's generally lower overhead elsewhere.
- **8GB RAM** — no GPU was used or available for any measured task, consistent with the assignment's requirement that GPU acceleration must not be enabled.

## 7. Submission structure

```
A1_2023341/
├── README.md                  <- this file
├── data_prep/prepare_data.sql
├── mapreduce/task1.py, task4.py
├── spark/task1.py, task2.py, task2_explain.txt, task3.py, task3_explain.txt, task4.py, task5.py
├── duckdb/task1.py, task1.sql, task2.py, task2.sql, task2_explain.txt, task3.py, task3.sql, task4.py, task5.py
├── results/timings.csv, scaling_plot.png, make_scaling_plot.py, merge_timings.py, task*_raw.csv
├── ai/ai_usage.md
└── report/observations.pdf
```

Note: the local `data/` folder (raw taxi datasets, generated samples) is **excluded** from the submission ZIP per assignment rules — it must be recreated locally using Section 3 above before running any script.
