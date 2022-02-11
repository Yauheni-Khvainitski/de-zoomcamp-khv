  -- Question 1
  -- What is count for fhv vehicles data for year 2019

SELECT
  COUNT(*)
FROM
  `de-zoomcamp-khv.trips_data_all.fhv_ext` AS t
WHERE
  CAST(t.pickup_datetime AS DATE) BETWEEN CAST('2019-01-01' AS DATE)
  AND CAST('2019-12-31' AS DATE);
  -- 42 084 899
  -- 321.1 MB processed
  -- check with partitioned table

CREATE OR REPLACE TABLE
  `de-zoomcamp-khv.trips_data_all.fhv_trips`
PARTITION BY
  TIMESTAMP_TRUNC(pickup_datetime, year) OPTIONS (require_partition_filter=TRUE) AS
SELECT
  *
FROM
  `de-zoomcamp-khv.trips_data_all.fhv_ext`;
  -- 2.3 GB processed

SELECT
  COUNT(*)
FROM
  `de-zoomcamp-khv.trips_data_all.fhv_ext` AS t
WHERE
  CAST(t.pickup_datetime AS DATE) BETWEEN CAST('2021-01-01' AS DATE)
  AND CAST('2021-12-31' AS DATE);
  
SELECT
  *
FROM
  `de-zoomcamp-khv.trips_data_all.INFORMATION_SCHEMA.PARTITIONS`
WHERE
  table_name = 'fhv_2019_part_clust';

  -- Question 3
CREATE OR REPLACE TABLE
  `de-zoomcamp-khv.trips_data_all.fhv_part_clust`
PARTITION BY
  TIMESTAMP_TRUNC(dropoff_datetime, month)
CLUSTER BY
  dispatching_base_num 
-- OPTIONS (require_partition_filter=TRUE) 
AS
SELECT
  *
FROM
  `de-zoomcamp-khv.trips_data_all.fhv_ext`;

  -- not partitioned and not clustered table for comparison
CREATE OR REPLACE TABLE
  `de-zoomcamp-khv.trips_data_all.fhv_non_opt` AS
SELECT
  *
FROM
  `de-zoomcamp-khv.trips_data_all.fhv_ext`;



  -- Question 4
SELECT COUNT(*) 
  FROM `de-zoomcamp-khv.trips_data_all.fhv_part_clust` 
 WHERE 
    CAST(dropoff_datetime AS DATE) 
      BETWEEN CAST('2019-01-01' AS DATE) AND CAST('2019-12-31' AS DATE)
  AND dispatching_base_num IN ('B00987', 'B02060', 'B02279');
-- count 126 777
-- estim 643MB
-- actual 154MB

SELECT COUNT(*) 
  FROM `de-zoomcamp-khv.trips_data_all.fhv_non_opt` 
 WHERE 
    CAST(dropoff_datetime AS DATE) 
      BETWEEN CAST('2019-01-01' AS DATE) AND CAST('2019-12-31' AS DATE)
  AND dispatching_base_num IN ('B00987', 'B02060', 'B02279');
-- count 126 777
-- estim 973.7MB
-- actual 973.7MB



-- Question 5
select count(distinct SR_Flag) from de-zoomcamp-khv.trips_data_all.fhv_part_clust;
-- 43 distinct SR Flags
-- 914 dist dispatch numbers
-- should be clustred by sr flag first and then by dispatch base


CREATE OR REPLACE TABLE
  `de-zoomcamp-khv.trips_data_all.fhv_part_clust_2`
PARTITION BY
  TIMESTAMP_TRUNC(dropoff_datetime, month)
CLUSTER BY
  SR_Flag, dispatching_base_num 
-- OPTIONS (require_partition_filter=TRUE) 
AS
SELECT
  *
FROM
  `de-zoomcamp-khv.trips_data_all.fhv_ext`;

select * from trips_data_all.fhv_part_clust_2 limit 1;


