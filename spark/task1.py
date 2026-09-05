# AI-ASSISTED: A1
"""
Task 1 - PySpark DataFrame implementation.
SparkSession is created before the timed region, per §5.3.
"""
import argparse
import time
from pyspark.sql import SparkSession, functions as F


def build_query(spark, input_path):
    df = spark.read.csv(input_path, header=True, inferSchema=False)
    df = (
        df.withColumn('PULocationID', F.col('PULocationID').cast('int'))
          .withColumn('fare_amount', F.col('fare_amount').cast('double'))
    )
    return (
        df.filter(F.col('PULocationID').isNotNull())
          .filter(F.col('fare_amount').isNotNull())
          .filter(F.col('fare_amount') >= 0)
          .groupBy('PULocationID')
          .agg(
              F.count(F.lit(1)).alias('trip_count'),
              F.avg('fare_amount').alias('avg_fare'),
          )
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', default='data/taxi_1m.csv')
    args = parser.parse_args()

    spark = SparkSession.builder.appName('Task1').master('local[*]').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    result_df = build_query(spark, args.input)  # still lazy, not timed

    times, rows = [], None
    for run in range(4):  # run 0 = warm-up
        start = time.perf_counter()
        rows = result_df.collect()  # materializes -> this is what's timed
        elapsed = time.perf_counter() - start
        if run == 0:
            print(f'Warm-up run: {elapsed:.4f} s (not recorded)')
        else:
            times.append(elapsed)
            print(f'Run {run}: {elapsed:.4f} s')

    print('\nRecorded times (s):', [f'{t:.4f}' for t in times])
    print(f'Median: {sorted(times)[1]:.4f} s')

    print('\nSample of results:')
    for row in rows[:10]:
        print(row)

    spark.stop()


if __name__ == '__main__':
    main()