CREATE OR REPLACE TABLE db_devfestgoogle.bike_sample AS
SELECT
  t.trip_id,
  t.duration_sec,
  t.start_station_id,
  t.end_station_id,
  t.start_date,
  t.end_date,
  t.bike_number,
  t.subscriber_type,
  t.zip_code,

  -- duración en minutos
  CAST(duration_sec/60 AS FLOAT64) AS duration_min,

  -- distancia entre estaciones
  ST_DISTANCE(
    ST_GEOGPOINT(s1.longitude, s1.latitude),
    ST_GEOGPOINT(s2.longitude, s2.latitude)
  ) AS distance_meters,

  -- etiqueta binaria usando cutoff de 9 minutos
  CASE WHEN duration_sec/60 > 9 THEN 1 ELSE 0 END AS long_trip

FROM `bigquery-public-data.san_francisco.bikeshare_trips` t
LEFT JOIN `bigquery-public-data.san_francisco.bikeshare_stations` s1
  ON t.start_station_id = s1.station_id
LEFT JOIN `bigquery-public-data.san_francisco.bikeshare_stations` s2
  ON t.end_station_id = s2.station_id

WHERE duration_sec > 0
  AND ABS(MOD(FARM_FINGERPRINT(CAST(t.trip_id AS STRING)), 100)) < 40;