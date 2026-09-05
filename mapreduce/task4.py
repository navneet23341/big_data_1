# AI-ASSISTED: A4
"""
Task 4 - scaling experiment for MapReduce (mrjob).
Runs mapreduce/task1.py as a subprocess for each dataset size,
1 warm-up + 3 timed runs, with a 900s timeout per run (§5.7).
"""
import subprocess
import time
import csv
import sys

DATASETS = [
    (100_000, 'data/taxi_100k.csv'),
    (500_000, 'data/taxi_500k.csv'),
    (1_000_000, 'data/taxi_1m.csv'),
    (2_000_000, 'data/taxi_2m.csv'),
]

TIMEOUT_SECONDS = 900
OUTPUT_CSV = 'results/task4_mapreduce_raw.csv'


def run_once(csv_path):
    start = time.perf_counter()
    try:
        subprocess.run(
            [sys.executable, 'mapreduce/task1.py', '-r', 'local', csv_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=TIMEOUT_SECONDS,
            check=True,
        )
        return time.perf_counter() - start, 'OK'
    except subprocess.TimeoutExpired:
        return TIMEOUT_SECONDS, 'TIMEOUT_900'
    except subprocess.CalledProcessError as e:
        elapsed = time.perf_counter() - start
        with open('results/errors.txt', 'a') as f:
            f.write(f'mapreduce task4 {csv_path}: exited with {e.returncode}\n')
        return elapsed, 'ERROR'


def main():
    rows = []
    for n_rows, path in DATASETS:
        print(f'\n=== MapReduce, {n_rows} rows ({path}) ===')

        elapsed, status = run_once(path)  # warm-up, not recorded
        print(f'Warm-up: {elapsed:.3f}s status={status}')
        if status == 'TIMEOUT_900':
            rows.append({'dataset_rows': n_rows, 'run_number': 1,
                         'time_seconds': TIMEOUT_SECONDS, 'status': 'TIMEOUT_900'})
            continue

        for run_number in range(1, 4):
            elapsed, status = run_once(path)
            print(f'Run {run_number}: {elapsed:.3f}s status={status}')
            rows.append({'dataset_rows': n_rows, 'run_number': run_number,
                         'time_seconds': round(elapsed, 4), 'status': status})
            if status == 'TIMEOUT_900':
                break  # §5.7: do not repeat a timed-out combination

    with open(OUTPUT_CSV, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['dataset_rows', 'run_number', 'time_seconds', 'status'])
        w.writeheader()
        w.writerows(rows)
    print(f'\nSaved raw timings to {OUTPUT_CSV}')


if __name__ == '__main__':
    main()