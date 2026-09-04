"""
Builds the Task 4 scaling plot from the three raw timing CSVs.
"""
import csv
from collections import defaultdict
import matplotlib.pyplot as plt

FILES = {
    'MapReduce': 'results/task4_mapreduce_raw.csv',
    'Spark': 'results/task4_spark_raw.csv',
    'DuckDB': 'results/task4_duckdb_raw.csv',
}


def load_medians(path):
    by_rows = defaultdict(list)
    timeout_rows = set()
    with open(path) as f:
        for row in csv.DictReader(f):
            n = int(row['dataset_rows'])
            t = float(row['time_seconds'])
            by_rows[n].append(t)
            if row['status'] == 'TIMEOUT_900':
                timeout_rows.add(n)
    medians = {}
    for n, times in by_rows.items():
        times.sort()
        medians[n] = times[len(times) // 2]
    return medians, timeout_rows


def main():
    plt.figure(figsize=(8, 5))
    colors = {'MapReduce': 'tab:red', 'Spark': 'tab:blue', 'DuckDB': 'tab:green'}

    for system, path in FILES.items():
        medians, timeouts = load_medians(path)
        xs = sorted(medians.keys())
        ys = [medians[x] for x in xs]
        plt.plot(xs, ys, marker='o', label=system, color=colors[system])
        for x in xs:
            if x in timeouts:
                plt.scatter([x], [medians[x]], marker='x', s=150,
                            color=colors[system], zorder=5)

    plt.xlabel('Dataset rows')
    plt.ylabel('Median execution time (seconds)')
    plt.title('Task 4 — Scaling: MapReduce vs Spark vs DuckDB')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('results/scaling_plot.png', dpi=150)
    print('Saved results/scaling_plot.png')


if __name__ == '__main__':
    main()