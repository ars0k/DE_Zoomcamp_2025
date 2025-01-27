--Q3
--During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), 
--how many trips, respectively, happened:

--    Up to 1 mile
SELECT 
	count(*)
FROM 
	green_taxi_data 
where 
	lpep_pickup_datetime >= '2019-10-01' and
	lpep_pickup_datetime <'2019-11-01' and
	lpep_dropoff_datetime >= '2019-10-01' and
	lpep_dropoff_datetime <'2019-11-01' and
	trip_distance <= 1;
--104802

--    In between 1 (exclusive) and 3 miles (inclusive)
SELECT 
	count(*)
FROM 
	green_taxi_data 
where 
	lpep_pickup_datetime >= '2019-10-01' and
	lpep_pickup_datetime <'2019-11-01' and
	lpep_dropoff_datetime >= '2019-10-01' and
	lpep_dropoff_datetime <'2019-11-01' and
	trip_distance > 1 and
	trip_distance <= 3;
--198924

--Obviously second answer is correct - 104,802; 198,924; 109,603; 27,678; 35,189

--	In between 3 (exclusive) and 7 miles (inclusive)
SELECT 
	count(*)
FROM 
	green_taxi_data 
where 
	lpep_pickup_datetime >= '2019-10-01' and
	lpep_pickup_datetime <'2019-11-01' and
	lpep_dropoff_datetime >= '2019-10-01' and
	lpep_dropoff_datetime <'2019-11-01' and
	trip_distance > 3 and
	trip_distance <= 7;
--109603

--	In between 7 (exclusive) and 10 miles (inclusive)
SELECT 
	count(*)
FROM 
	green_taxi_data 
where 
	lpep_pickup_datetime >= '2019-10-01' and
	lpep_pickup_datetime <'2019-11-01' and
	lpep_dropoff_datetime >= '2019-10-01' and
	lpep_dropoff_datetime <'2019-11-01' and
	trip_distance > 7 and
	trip_distance <= 10;
--27678
	
--  Over 10 miles
SELECT 
	count(*)
FROM 
	green_taxi_data 
where 
	lpep_pickup_datetime >= '2019-10-01' and
	lpep_pickup_datetime <'2019-11-01' and
	lpep_dropoff_datetime >= '2019-10-01' and
	lpep_dropoff_datetime <'2019-11-01' and
	trip_distance > 10;
--35189


--   104,802; 197,670; 110,612; 27,831; 35,281
--   104,802; 198,924; 109,603; 27,678; 35,189 - correct one!
--   104,793; 201,407; 110,612; 27,831; 35,281
--   104,793; 202,661; 109,603; 27,678; 35,189
--   104,838; 199,013; 109,645; 27,688; 35,202


-------------------------------------------------------------------------------------------------------------------------------------------
--Q4
--Longest trip for each day
--Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.
--Tip: For every day, we only care about one single trip with the longest distance.

select
	date_trunc('day', lpep_pickup_datetime)::date,
	max(trip_distance)
from
	green_taxi_data
group by 1
order by 2 desc
limit 1;

--2019-10-31



--   2019-10-11
--   2019-10-24
--   2019-10-26
--   2019-10-31 - correct one!

-------------------------------------------------------------------------------------------------------------------------------------------
--Q5
--Three biggest pickup zones

--Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?
--Consider only lpep_pickup_datetime when filtering by date.

select
	z."Zone",
	sum(total_amount)
from
	green_taxi_data t
join
	taxi_zone_lookup z
on t."PULocationID" = z."LocationID"
where
	lpep_pickup_datetime::date = '2019-10-18'
group by 1
having sum(total_amount)>13000
order by 2 desc
limit 3;


--    East Harlem North, East Harlem South, Morningside Heights - correct one!
--    East Harlem North, Morningside Heights
--    Morningside Heights, Astoria Park, East Harlem South
--    Bedford, East Harlem North, Astoria Park

-------------------------------------------------------------------------------------------------------------------------------------------
--Q6
--Largest tip

--For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?
--Note: it's tip , not trip
--We need the name of the zone, not the ID.

select
	zdo."Zone",
	tip_amount
from
	green_taxi_data t
join
	taxi_zone_lookup zpu
		on t."PULocationID" = zpu."LocationID"
		and zpu."Zone" = 'East Harlem North'
join 
	taxi_zone_lookup zdo
		on t."DOLocationID" = zdo."LocationID"
where
	lpep_pickup_datetime >= '2019-10-01' and
	lpep_pickup_datetime < '2019-11-01' 
order by 2 desc
limit 1;


--    Yorkville West
--    JFK Airport  - correct one!
--    East Harlem North
--    East Harlem South
