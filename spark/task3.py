import argparse
import time
import io, contextlib
from pyspark.sql import SparkSession, functions as F


def build_query(spark, trips_path, zones_path):
    trips = spark.read.parquet(trips_path)
    zones = spark.read.csv(zones_path, header=True, inferSchema=True)

    joined = trips.join(zones, trips.PULocationID == zones.LocationID, 'inner')

    return (
        joined.filter(F.col('fare_amount') >= 0)
        .groupBy('Borough', 'Zone')
        .agg(F.sum('fare_amount').alias('total_fare'))
        .orderBy(F.col('total_fare').desc(), F.col('Zone').asc())
        .limit(5)
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trips', default='data/taxi_2m.parquet')
    parser.add_argument('--zones', default='data/taxi_zone_lookup.csv')
    args = parser.parse_args()

    spark = SparkSession.builder.appName('Task3').master('local[*]').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    times = []
    result = None
    for run in range(4):
        start = time.perf_counter()
        df = build_query(spark, args.trips, args.zones)
        result = df.collect()  # materializes
        elapsed = time.perf_counter() - start
        if run == 0:
            print(f'Warm-up: {elapsed:.4f} s (not recorded)')
        else:
            times.append(elapsed)
            print(f'Run {run}: {elapsed:.4f} s')

    print(f'Median: {sorted(times)[1]:.4f} s')
    print('Top 5:')
    for row in result:
        print(row)

    # Save physical plan to inspect broadcast strategy (do NOT force broadcast)
    with open('spark/task3_explain.txt', 'w') as f:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            build_query(spark, args.trips, args.zones).explain(mode='formatted')
        f.write(buf.getvalue())

    spark.stop()


if __name__ == '__main__':
    main()