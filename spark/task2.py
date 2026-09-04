import argparse
import time
from pyspark.sql import SparkSession, functions as F




def run_timed(df_builder, label, n_runs=4):
    times = []
    result_count = None
    for run in range(n_runs):
        start = time.perf_counter()
        df = df_builder()
        result_count = df.count()  # forces materialization
        rows = df.take(5)          # small sample only, per §5.6
        elapsed = time.perf_counter() - start
        if run == 0:
            print(f'[{label}] Warm-up: {elapsed:.4f} s (not recorded)')
        else:
            times.append(elapsed)
            print(f'[{label}] Run {run}: {elapsed:.4f} s')
    print(f'[{label}] rows={result_count}, median={sorted(times)[1]:.4f} s\n')
    return times, rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', default='data/taxi_2m.parquet')
    args = parser.parse_args()

    spark = SparkSession.builder.appName('Task2').master('local[*]').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    FILTER_COND = (F.col('PULocationID') == 161) & (F.col('fare_amount') > 30.00)

    def query_a():
        return spark.read.parquet(args.input).filter(FILTER_COND)

    def query_b():
        return (
            spark.read.parquet(args.input)
            .filter(FILTER_COND)
            .select('tpep_pickup_datetime', 'tpep_dropoff_datetime',
                     'trip_distance', 'fare_amount')
        )

    times_a, sample_a = run_timed(query_a, 'Query A (all columns)')
    times_b, sample_b = run_timed(query_b, 'Query B (4 columns)')

    print('Sample A:', sample_a[:3])
    print('Sample B:', sample_b[:3])

    # Save formatted physical plans (§8 requirement)
    with open('spark/task2_explain.txt', 'w') as f:
        f.write('=== Query A explain (formatted) ===\n')
        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            query_a().explain(mode='formatted')
        f.write(buf.getvalue())

        f.write('\n\n=== Query B explain (formatted) ===\n')
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            query_b().explain(mode='formatted')
        f.write(buf.getvalue())

    spark.stop()


if __name__ == '__main__':
    main()