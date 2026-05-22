-- Monthly average total_amount (Jan–May 2023)
SELECT
  date_format(tpep_pickup_datetime, 'MM')  AS trip_month,
  ROUND(AVG(total_amount),2) AS avg_total_amount
FROM nyc_taxi.yellow_trips_consumption
GROUP BY 1
ORDER BY 1;

-- Average passenger_count by hour of day in May 2023
SELECT
  hour(tpep_pickup_datetime) AS pickup_hour,
  ROUND(AVG(passenger_count),2) AS avg_passenger_count
FROM nyc_taxi.yellow_trips_consumption
WHERE tpep_pickup_datetime >= '2023-05-01'
  AND tpep_pickup_datetime < '2023-06-01'
GROUP BY 1
ORDER BY 1;