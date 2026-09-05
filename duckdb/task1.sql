-- AI-ASSISTED: A1
-- Task 1: aggregation by PULocationID
SELECT
    PULocationID,
    COUNT(*)          AS trip_count,
    AVG(fare_amount)  AS avg_fare
FROM read_csv_auto('taxi_1m.csv', header = true)
WHERE PULocationID IS NOT NULL
  AND fare_amount IS NOT NULL
  AND fare_amount >= 0
GROUP BY PULocationID
ORDER BY PULocationID;