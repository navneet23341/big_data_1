"""
Task 1 - MapReduce (mrjob) implementation.

Mapper key   : PULocationID (int)
Mapper value : (fare_amount, 1)   -- a partial sum and a partial count
Shuffle      : for each distinct PULocationID key, the shuffle groups together
               every (fare_amount, 1) pair emitted by every mapper for that key,
               so the reducer receives an iterator of (fare, 1) tuples per key.
Reducer      : sums the fares and counts per key -> trip_count, avg_fare
"""
import csv
from io import StringIO
from mrjob.job import MRJob
from mrjob.step import MRStep

# --- Adjust these two if `head -1 data/taxi_1m.csv` shows different positions ---
PULOCATIONID_IDX = 7
FARE_AMOUNT_IDX = 10


class MRAvgFareByPULocation(MRJob):

    def mapper(self, _, line):
        try:
            row = next(csv.reader(StringIO(line)), None)
        except csv.Error:
            return
        if row is None or len(row) <= max(PULOCATIONID_IDX, FARE_AMOUNT_IDX):
            return

        # Skip header row wherever it appears (only one mapper task will see it)
        if row[0] == 'VendorID':
            return

        pu_raw = row[PULOCATIONID_IDX]
        fare_raw = row[FARE_AMOUNT_IDX]

        if pu_raw is None or pu_raw.strip() == '' or pu_raw.strip().upper() == 'NULL':
            return
        if fare_raw is None or fare_raw.strip() == '' or fare_raw.strip().upper() == 'NULL':
            return

        try:
            pu_location_id = int(float(pu_raw))
            fare_amount = float(fare_raw)
        except ValueError:
            return

        if fare_amount < 0:
            return

        yield pu_location_id, (fare_amount, 1)

    def combiner(self, pu_location_id, fare_count_pairs):
        total_fare, total_count = 0.0, 0
        for fare, count in fare_count_pairs:
            total_fare += fare
            total_count += count
        yield pu_location_id, (total_fare, total_count)

    def reducer(self, pu_location_id, fare_count_pairs):
        total_fare, total_count = 0.0, 0
        for fare, count in fare_count_pairs:
            total_fare += fare
            total_count += count
        avg_fare = total_fare / total_count if total_count > 0 else 0.0
        yield None, {
            "PULocationID": pu_location_id,
            "trip_count": total_count,
            "avg_fare": avg_fare,
        }

    def steps(self):
        return [MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer)]


if __name__ == '__main__':
    MRAvgFareByPULocation.run()