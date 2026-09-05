# AI-ASSISTED: A3
import time
import argparse
import duckdb

QUERY = """
SELECT z.Borough, z.Zone, SUM(t.fare_amount) AS total_fare
FROM read_parquet(?) t
JOIN read_csv_auto(?, header = true) z
  ON t.PULocationID = z.LocationID
WHERE t.fare_amount >= 0
GROUP BY z.Borough, z.Zone
ORDER BY total_fare DESC, z.Zone ASC
LIMIT 5;
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trips', default='data/taxi_2m.parquet')
    parser.add_argument('--zones', default='data/taxi_zone_lookup.csv')
    args = parser.parse_args()

    con = duckdb.connect()

    times = []
    result = None
    for run in range(4):
        start = time.perf_counter()
        result = con.execute(QUERY, [args.trips, args.zones]).fetchall()
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

    con.close()


if __name__ == '__main__':
    main()