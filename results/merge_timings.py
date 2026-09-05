# AI-ASSISTED: A5, A6
"""
results/merge_timings.py
Combines ALL raw per-task timing CSVs (Task 1-5) into results/timings.csv per §14.
Fill in MACHINE CONFIG / STUDENT_ID / TOOL_VERSIONS once at the top.
"""
import csv
import os
import platform

# ============ FILL THESE IN ONCE ============
STUDENT_ID = "2023341"

MACHINE_CONFIG = {
    "os": "Linux Mint 22.3",
    "cpu_model": "11th Gen Intel(R) Core(TM) i3-1115G4",
    "cpu_base_ghz": "3.00",
    "cpu_physical_cores": "2",
    "cpu_logical_processors": "4",
    "ram_gb": "8",
    "storage_size_gb": "64",
    "storage_type": "Other (Live USB)",
    "gpu_model": "Integrated",
    "gpu_clock_mhz": "NA",
    "gpu_memory_gb": "NA",
}

PYTHON_VERSION = platform.python_version()

TOOL_VERSIONS = {
    "mapreduce": "0.7.4",
    "spark": "4.2.0",
    "duckdb": "1.5.5",
}
# ==============================================

OUTPUT = "results/timings.csv"

FIELDNAMES = [
    "student_id", "system", "task", "condition", "dataset_rows",
    "input_format", "input_mb", "run_number", "time_seconds", "status",
    "os", "cpu_model", "cpu_base_ghz", "cpu_physical_cores",
    "cpu_logical_processors", "ram_gb", "storage_size_gb", "storage_type",
    "gpu_model", "gpu_clock_mhz", "gpu_memory_gb",
    "python_version", "tool_version",
]


def file_mb(path):
    if os.path.exists(path):
        return round(os.path.getsize(path) / (1024 * 1024), 2)
    return "NA"


INPUT_MB = {
    (100_000, "csv"): file_mb("data/taxi_100k.csv"),
    (100_000, "parquet"): file_mb("data/taxi_100k.parquet"),
    (500_000, "csv"): file_mb("data/taxi_500k.csv"),
    (500_000, "parquet"): file_mb("data/taxi_500k.parquet"),
    (1_000_000, "csv"): file_mb("data/taxi_1m.csv"),
    (1_000_000, "parquet"): file_mb("data/taxi_1m.parquet"),
    (2_000_000, "csv"): file_mb("data/taxi_2m.csv"),
    (2_000_000, "parquet"): file_mb("data/taxi_2m.parquet"),
}


def base_row(system, task, condition, dataset_rows, input_format, run_number, time_seconds, status):
    row = {
        "student_id": STUDENT_ID,
        "system": system,
        "task": task,
        "condition": condition,
        "dataset_rows": dataset_rows,
        "input_format": input_format,
        "input_mb": INPUT_MB.get((dataset_rows, input_format), "NA"),
        "run_number": run_number,
        "time_seconds": time_seconds,
        "status": status,
        "python_version": PYTHON_VERSION,
        "tool_version": TOOL_VERSIONS.get(system, "TODO"),
    }
    row.update(MACHINE_CONFIG)
    return row


def load_simple(path, system, task, condition, dataset_rows, input_format):
    """For raw CSVs with columns: run_number,time_seconds,status"""
    rows = []
    if not os.path.exists(path):
        print(f"  [skip] {path} not found")
        return rows
    with open(path) as f:
        for r in csv.DictReader(f):
            rows.append(base_row(system, task, condition, dataset_rows, input_format,
                                  int(r["run_number"]), r["time_seconds"], r["status"]))
    return rows


def load_with_condition_col(path, system, task, dataset_rows, input_format):
    """For raw CSVs with columns: condition,run_number,time_seconds,status"""
    rows = []
    if not os.path.exists(path):
        print(f"  [skip] {path} not found")
        return rows
    with open(path) as f:
        for r in csv.DictReader(f):
            rows.append(base_row(system, task, r["condition"], dataset_rows, input_format,
                                  int(r["run_number"]), r["time_seconds"], r["status"]))
    return rows


def load_task4(system, path):
    """Task 4 raw CSVs have columns: dataset_rows,run_number,time_seconds,status"""
    rows = []
    if not os.path.exists(path):
        print(f"  [skip] {path} not found")
        return rows
    with open(path) as f:
        for r in csv.DictReader(f):
            n = int(r["dataset_rows"])
            rows.append(base_row(system, "task4", "aggregation", n, "csv",
                                  int(r["run_number"]), r["time_seconds"], r["status"]))
    return rows


def main():
    all_rows = []

    print("Loading Task 1 (taxi_1m.csv aggregation)...")
    all_rows += load_simple("results/task1_mapreduce_raw.csv", "mapreduce", "task1", "aggregation", 1_000_000, "csv")
    all_rows += load_simple("results/task1_spark_raw.csv", "spark", "task1", "aggregation", 1_000_000, "csv")
    all_rows += load_simple("results/task1_duckdb_raw.csv", "duckdb", "task1", "aggregation", 1_000_000, "csv")

    print("Loading Task 2 (taxi_2m.parquet filter/projection)...")
    all_rows += load_with_condition_col("results/task2_spark_raw.csv", "spark", "task2", 2_000_000, "parquet")
    all_rows += load_with_condition_col("results/task2_duckdb_raw.csv", "duckdb", "task2", 2_000_000, "parquet")

    print("Loading Task 3 (taxi_2m.parquet join)...")
    all_rows += load_simple("results/task3_spark_raw.csv", "spark", "task3", "top5_join", 2_000_000, "parquet")
    all_rows += load_simple("results/task3_duckdb_raw.csv", "duckdb", "task3", "top5_join", 2_000_000, "parquet")

    print("Loading Task 4 (scaling)...")
    all_rows += load_task4("mapreduce", "results/task4_mapreduce_raw.csv")
    all_rows += load_task4("spark", "results/task4_spark_raw.csv")
    all_rows += load_task4("duckdb", "results/task4_duckdb_raw.csv")

    print("Loading Task 5 (repeated queries)...")
    all_rows += load_with_condition_col("results/task5_spark_raw.csv", "spark", "task5", 1_000_000, "parquet")
    all_rows += load_with_condition_col("results/task5_duckdb_raw.csv", "duckdb", "task5", 1_000_000, "parquet")

    with open(OUTPUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(all_rows)

    print(f"\nWrote {len(all_rows)} rows to {OUTPUT}")
    todo_count = sum(1 for v in {**MACHINE_CONFIG, **TOOL_VERSIONS}.values() if v == "TODO")
    if todo_count or STUDENT_ID == "2026XXXX":
        print(f"!! WARNING: {todo_count} TODO field(s) + student_id still unfilled. Edit the top of this script and re-run. !!")


if __name__ == "__main__":
    main()