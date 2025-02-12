--Q1
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny-111.de_zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://taxi-rides-ny-111-module-3/yellow_tripdata_2024-*.parquet']
);

-- Create a regular
CREATE OR REPLACE TABLE taxi-rides-ny-111.de_zoomcamp.e_yellow_tripdata_regular AS
SELECT * FROM taxi-rides-ny-111.de_zoomcamp.external_yellow_tripdata;

-- Show records (data only between Jan - Jun 2024)
SELECT COUNT(*)
FROM taxi-rides-ny-111.de_zoomcamp.external_yellow_tripdata;

--Q2

-- Distinct PULocationIDs External
SELECT COUNT(DISTINCT(PULocationID))
FROM taxi-rides-ny-111.de_zoomcamp.external_yellow_tripdata;

-- Distinct PULocationIDs Materialized
SELECT COUNT(DISTINCT(PULocationID))
FROM taxi-rides-ny-111.de_zoomcamp.e_yellow_tripdata_regular;

--Then check the 'Job Information' Bytes processed to get info about processed data

--Q3
-- PULocationIDs Materialized
SELECT PULocationID
FROM taxi-rides-ny-111.de_zoomcamp.e_yellow_tripdata_regular;

-- PULocationIDs, DOLocationID Materialized
SELECT PULocationID, DOLocationID
FROM taxi-rides-ny-111.de_zoomcamp.e_yellow_tripdata_regular;

--Q4
-- Count fare_amount = 0
SELECT Count(1)
FROM taxi-rides-ny-111.de_zoomcamp.e_yellow_tripdata_regular
WHERE fare_amount = 0;

--Q5
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE taxi-rides-ny-111.de_zoomcamp.pc_yellow_tripdata_regular
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM taxi-rides-ny-111.de_zoomcamp.e_yellow_tripdata_regular;

--Q6

-- Distinct VendorIDs Materialized
SELECT DISTINCT(VendorID)
FROM taxi-rides-ny-111.de_zoomcamp.e_yellow_tripdata_regular
WHERE tpep_dropoff_datetime >= '2024-03-01' and tpep_dropoff_datetime <= '2024-03-15'


-- Distinct VendorIDs Materialized Partitioned and clustered
SELECT DISTINCT(VendorID)
FROM taxi-rides-ny-111.de_zoomcamp.pc_yellow_tripdata_regular
WHERE tpep_dropoff_datetime >= '2024-03-01' and tpep_dropoff_datetime <= '2024-03-15'
