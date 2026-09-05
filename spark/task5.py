# AI-ASSISTED: A4
"""
Task 5 - repeated work on the same data (Spark).
"""
import time
import csv
from pyspark.sql import SparkSession, functions as F

INPUT = 'data/taxi_1m.parquet'
OUTPUT_CSV = 'results/task5_spark_raw.csv'


def build_query(df):
    return (
        df.filter(F.col('PULocationID').isNotNull())
          .filter(F.col('fare_amount').isNotNull())
          .filter(F.col('fare_amount') >= 0)
          .groupBy('PULocationID')
          .agg(F.count(F.lit(1)).alias('trip_count'), F.avg('fare_amount').alias('avg_fare'))
    )


def main():
    spark = SparkSession.builder.appName('Task5').master('local[*]').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    rows = []

    # --- 1. Uncached: load once, run aggregation 5 times, no cache/persist ---
    print('=== Spark UNCACHED ===')
    df_uncached = spark.read.parquet(INPUT)
    for run_number in range(1, 6):
        start = time.perf_counter()
        build_query(df_uncached).collect()
        elapsed = time.perf_counter() - start
        print(f'Uncached run {run_number}: {elapsed:.4f}s')
        rows.append({'condition': 'spark_uncached', 'run_number': run_number,
                     'time_seconds': round(elapsed, 4), 'status': 'OK'})

    # --- 2. Cached: cache, force materialize with count(), timed SEPARATELY ---
    print('\n=== Spark CACHED ===')
    df_cached = spark.read.parquet(INPUT).cache()

    start = time.perf_counter()
    n = df_cached.count()  # forces the cache to materialize
    cache_build_elapsed = time.perf_counter() - start
    print(f'Cache build (count={n}): {cache_build_elapsed:.4f}s  [NOT one of the 5 runs]')
    rows.append({'condition': 'spark_cache_build', 'run_number': 0,
                 'time_seconds': round(cache_build_elapsed, 4), 'status': 'OK'})

    for run_number in range(1, 6):
        start = time.perf_counter()
        build_query(df_cached).collect()
        elapsed = time.perf_counter() - start
        print(f'Cached run {run_number}: {elapsed:.4f}s')
        rows.append({'condition': 'spark_cached', 'run_number': run_number,
                     'time_seconds': round(elapsed, 4), 'status': 'OK'})

    with open(OUTPUT_CSV, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['condition', 'run_number', 'time_seconds', 'status'])
        w.writeheader()
        w.writerows(rows)
    print(f'\nSaved raw timings to {OUTPUT_CSV}')

    df_cached.unpersist()
    spark.stop()


if __name__ == '__main__':
    main()