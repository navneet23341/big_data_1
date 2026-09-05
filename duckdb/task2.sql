-- AI-ASSISTED: A3
-- Task 2, Query A: all columns
SELECT *
FROM read_parquet('taxi_2m.parquet')
WHERE PULocationID = 161 AND fare_amount > 30.00;

-- Task 2, Query B: 4 columns only
SELECT tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, fare_amount
FROM read_parquet('taxi_2m.parquet')
WHERE PULocationID = 161 AND fare_amount > 30.00;