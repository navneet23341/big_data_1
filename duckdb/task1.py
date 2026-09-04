import time
import argparse
import duckdb

QUERY = """
SELECT
    PULocationID,
    COUNT(*)          AS trip_count,
    AVG(fare_amount)  AS avg_fare
FROM read_csv_auto(?, header = true)
WHERE PULocationID IS NOT NULL
  AND fare_amount IS NOT NULL
  AND fare_amount >= 0
GROUP BY PULocationID
ORDER BY PULocationID;
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', default='data/taxi_1m.csv')
    args = parser.parse_args()

    con = duckdb.connect()  # connection opened before timed region

    times, result = [], None
    for run in range(4):
        start = time.perf_counter()
        result = con.execute(QUERY, [args.input]).fetchall()
        elapsed = time.perf_counter() - start
        if run == 0:
            print(f'Warm-up run: {elapsed:.4f} s (not recorded)')
        else:
            times.append(elapsed)
            print(f'Run {run}: {elapsed:.4f} s')

    print('\nRecorded times (s):', [f'{t:.4f}' for t in times])
    print(f'Median: {sorted(times)[1]:.4f} s')
    print('\nSample of results:')
    for row in result[:10]:
        print(row)

    con.close()


if __name__ == '__main__':
    main()