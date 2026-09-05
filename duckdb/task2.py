# AI-ASSISTED: A3
import time
import argparse
import duckdb

QUERY_A = """
SELECT * FROM read_parquet(?)
WHERE PULocationID = 161 AND fare_amount > 30.00;
"""

QUERY_B = """
SELECT tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, fare_amount
FROM read_parquet(?)
WHERE PULocationID = 161 AND fare_amount > 30.00;
"""


def run_timed(con, query, path, label, n_runs=4):
    times = []
    result = None
    for run in range(n_runs):
        start = time.perf_counter()
        result = con.execute(query, [path]).fetchall()
        elapsed = time.perf_counter() - start
        if run == 0:
            print(f'[{label}] Warm-up: {elapsed:.4f} s (not recorded)')
        else:
            times.append(elapsed)
            print(f'[{label}] Run {run}: {elapsed:.4f} s')
    print(f'[{label}] rows={len(result)}, median={sorted(times)[1]:.4f} s\n')
    return times, result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', default='data/taxi_2m.parquet')
    args = parser.parse_args()

    con = duckdb.connect()

    times_a, sample_a = run_timed(con, QUERY_A, args.input, 'Query A')
    times_b, sample_b = run_timed(con, QUERY_B, args.input, 'Query B')

    print('Sample A:', sample_a[:3])
    print('Sample B:', sample_b[:3])

    # Save EXPLAIN plans (§8 requirement)
    with open('duckdb/task2_explain.txt', 'w') as f:
        f.write('=== Query A EXPLAIN ===\n')
        plan_a = con.execute(f"EXPLAIN {QUERY_A.replace('?', repr(args.input))}").fetchall()
        for row in plan_a:
            f.write(str(row) + '\n')

        f.write('\n=== Query B EXPLAIN ===\n')
        plan_b = con.execute(f"EXPLAIN {QUERY_B.replace('?', repr(args.input))}").fetchall()
        for row in plan_b:
            f.write(str(row) + '\n')

    con.close()


if __name__ == '__main__':
    main()