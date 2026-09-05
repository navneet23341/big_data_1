# AI-ASSISTED: A4
"""
Task 5 - repeated work on the same data (DuckDB).
Same connection reused for all 5 runs (§11).
"""
import time
import csv
import duckdb

INPUT = 'data/taxi_1m.parquet'
OUTPUT_CSV = 'results/task5_duckdb_raw.csv'

QUERY = """
SELECT PULocationID, COUNT(*) AS trip_count, AVG(fare_amount) AS avg_fare
FROM read_parquet(?)
WHERE PULocationID IS NOT NULL AND fare_amount IS NOT NULL AND fare_amount >= 0
GROUP BY PULocationID;
"""


def main():
    con = duckdb.connect()

    rows = []
    print('=== DuckDB repeated queries ===')
    for run_number in range(1, 6):
        start = time.perf_counter()
        con.execute(QUERY, [INPUT]).fetchall()
        elapsed = time.perf_counter() - start
        print(f'Run {run_number}: {elapsed:.4f}s')
        rows.append({'condition': 'duckdb_repeated', 'run_number': run_number,
                     'time_seconds': round(elapsed, 4), 'status': 'OK'})

    with open(OUTPUT_CSV, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['condition', 'run_number', 'time_seconds', 'status'])
        w.writeheader()
        w.writerows(rows)
    print(f'\nSaved raw timings to {OUTPUT_CSV}')

    con.close()


if __name__ == '__main__':
    main()