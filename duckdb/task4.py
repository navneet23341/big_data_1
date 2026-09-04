"""
Task 4 - scaling experiment for DuckDB.
Connection opened once, before all timed regions (§5.3).
"""
import time
import csv
import signal
import duckdb

DATASETS = [
    (100_000, 'data/taxi_100k.csv'),
    (500_000, 'data/taxi_500k.csv'),
    (1_000_000, 'data/taxi_1m.csv'),
    (2_000_000, 'data/taxi_2m.csv'),
]

TIMEOUT_SECONDS = 900
OUTPUT_CSV = 'results/task4_duckdb_raw.csv'

QUERY = """
SELECT PULocationID, COUNT(*) AS trip_count, AVG(fare_amount) AS avg_fare
FROM read_csv_auto(?, header = true)
WHERE PULocationID IS NOT NULL AND fare_amount IS NOT NULL AND fare_amount >= 0
GROUP BY PULocationID;
"""


class _Timeout(Exception):
    pass


def _handler(signum, frame):
    raise _Timeout()


def run_once(con, path):
    signal.signal(signal.SIGALRM, _handler)
    signal.alarm(TIMEOUT_SECONDS)
    start = time.perf_counter()
    try:
        con.execute(QUERY, [path]).fetchall()
        elapsed = time.perf_counter() - start
        signal.alarm(0)
        return elapsed, 'OK'
    except _Timeout:
        signal.alarm(0)
        return TIMEOUT_SECONDS, 'TIMEOUT_900'
    except Exception as e:
        signal.alarm(0)
        elapsed = time.perf_counter() - start
        with open('results/errors.txt', 'a') as f:
            f.write(f'duckdb task4 {path}: {e}\n')
        return elapsed, 'ERROR'


def main():
    con = duckdb.connect()

    rows = []
    for n_rows, path in DATASETS:
        print(f'\n=== DuckDB, {n_rows} rows ({path}) ===')

        elapsed, status = run_once(con, path)
        print(f'Warm-up: {elapsed:.3f}s status={status}')
        if status == 'TIMEOUT_900':
            rows.append({'dataset_rows': n_rows, 'run_number': 1,
                         'time_seconds': TIMEOUT_SECONDS, 'status': 'TIMEOUT_900'})
            continue

        for run_number in range(1, 4):
            elapsed, status = run_once(con, path)
            print(f'Run {run_number}: {elapsed:.3f}s status={status}')
            rows.append({'dataset_rows': n_rows, 'run_number': run_number,
                         'time_seconds': round(elapsed, 4), 'status': status})
            if status == 'TIMEOUT_900':
                break

    with open(OUTPUT_CSV, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['dataset_rows', 'run_number', 'time_seconds', 'status'])
        w.writeheader()
        w.writerows(rows)
    print(f'\nSaved raw timings to {OUTPUT_CSV}')

    con.close()


if __name__ == '__main__':
    main()