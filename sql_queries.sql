-- rolling 7 day average revenue by zone
SELECT pu_location_id, pickup_datetime, AVERAGE(total_amount) AS average_revenue OVER (
ORDER BY pickup_datetime
ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
) AS rolling_7_day_avg
FROM nyc_taxi_staging 
GROUP BY pu_location_id
ORDER BY pickup_datetime; 

-- driver ranking by total revenue within each zone
SELECT driver_id, pu_location_id, SUM(total_amount) AS total_revenue 
FROM nyc_taxi_staging 
GROUP BY driver_id, pu_location_id
ORDER BY total_revenue DESC;

-- peak hour detection by location
SELECT EXTRACT(HOUR FROM pickup_datetime) AS peak_hour, pu_location_id
GROUP BY peak_hour, pu_location_id
FROM nyc_taxi_staging; 