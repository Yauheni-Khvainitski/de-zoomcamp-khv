-- question 3
-- check if there 01-15 not only in 2021
SELECT
    tpep_pickup_datetime::DATE
    , count(*)
FROM
    yellow_taxi_data
WHERE
    EXTRACT(
        DAY
    FROM
        tpep_pickup_datetime
    ) = 15
    AND EXTRACT(
        MONTH
    FROM
        tpep_pickup_datetime
    ) = 1
GROUP BY tpep_pickup_datetime::DATE;


-- question 4
-- window function is more optimal then IN MAX(tip amount in January)
SELECT
    dt
FROM
    (
        SELECT
            tpep_pickup_datetime::DATE AS dt
            , tip_amount
            , MAX(tip_amount) OVER(
                PARTITION BY DATE_TRUNC('month', tpep_pickup_datetime)
            ) AS mx
        FROM
            yellow_taxi_data
        WHERE
            EXTRACT(
                MONTH
            FROM
                tpep_pickup_datetime
            ) = 1
    ) sub
WHERE
    tip_amount = mx
    AND mx != 0;


-- question 5
SELECT
    COALESCE(zd."Zone", 'Unknown') AS do_zone
    , COUNT(*) cnt
FROM
    yellow_taxi_data r
LEFT JOIN taxi_zone_lookup zd ON
    r."DOLocationID" = zd."LocationID"
LEFT JOIN taxi_zone_lookup zp ON
    r."PULocationID" = zp."LocationID"
WHERE
    tpep_pickup_datetime >= '2021-01-14'
    AND tpep_pickup_datetime < '2021-01-15'
    AND COALESCE(zp."Zone", 'Unknown') = 'Central Park' -- avoid NULL filtering
GROUP BY
    COALESCE(zd."Zone", 'Unknown')
ORDER BY
    cnt DESC
LIMIT 1;



-- question 6
SELECT
    COALESCE(zp."Zone", 'Unknown') 
    || ' / ' || 
    COALESCE(zd."Zone", 'Unknown') AS dp_do_zones
    , AVG(r.total_amount) AS avg_amount
FROM
    yellow_taxi_data r
LEFT JOIN taxi_zone_lookup zd ON
    r."DOLocationID" = zd."LocationID"
LEFT JOIN taxi_zone_lookup zp ON
    r."PULocationID" = zp."LocationID"
GROUP BY
    zp."Zone"
    , zd."Zone"
ORDER BY avg_amount DESC
LIMIT 1;
    

