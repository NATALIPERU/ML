  -- ============================================================
  -- I. create_dataset_bike_sample
  -- ============================================================

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

  -- ============================================================
  -- II. split_bike_sample_train_test_backtest
  -- ============================================================

  -- ============================================================
  -- 1. OBTENER LOS CORTES TEMPORALES PARA TRAIN / TEST / BACKTEST
  -- ============================================================
  SELECT
    APPROX_QUANTILES(start_date, 100)[OFFSET(70)] AS cutoff_train,   -- 70%
    APPROX_QUANTILES(start_date, 100)[OFFSET(85)] AS cutoff_test     -- 85%
  FROM db_devfestgoogle.bike_sample;

  -- ===============================================
  -- 2. CREAR TABLA DE ENTRENAMIENTO (TRAIN - 70%)
  -- ===============================================
  CREATE OR REPLACE TABLE db_devfestgoogle.bike_sample_train AS
  SELECT *
  FROM db_devfestgoogle.bike_sample
  WHERE start_date < (
    SELECT APPROX_QUANTILES(start_date, 100)[OFFSET(70)]
    FROM db_devfestgoogle.bike_sample
  );

  -- ======================================
  -- 3. CREAR TABLA DE TEST (TEST - 15%)
  -- ======================================
  CREATE OR REPLACE TABLE db_devfestgoogle.bike_sample_test AS
  SELECT *
  FROM db_devfestgoogle.bike_sample
  WHERE start_date >= (
    SELECT APPROX_QUANTILES(start_date, 100)[OFFSET(70)]
    FROM db_devfestgoogle.bike_sample
  )
  AND start_date < (
    SELECT APPROX_QUANTILES(start_date, 100)[OFFSET(85)]
    FROM db_devfestgoogle.bike_sample
  );


  -- ===============================================
  -- 4. CREAR TABLA DE BACKTEST (BACKTEST - 15%)
  -- ===============================================
  CREATE OR REPLACE TABLE db_devfestgoogle.bike_sample_backtest AS
  SELECT *
  FROM db_devfestgoogle.bike_sample
  WHERE start_date >= (
    SELECT APPROX_QUANTILES(start_date, 100)[OFFSET(85)]
    FROM db_devfestgoogle.bike_sample
  );


  -- ============================================================
  -- III. train_bike_trip_model
  -- ============================================================
CREATE OR REPLACE MODEL db_devfestgoogle.bike_trip_model
OPTIONS(
  model_type = 'logistic_reg',
  input_label_cols = ['long_trip'],
  l1_reg = 0.0,
  l2_reg = 0.0,
  max_iterations = 50
) AS
SELECT
  -- variables categóricas
  start_station_id,
  end_station_id,
  subscriber_type,

  -- variables temporales
  EXTRACT(HOUR FROM start_date) AS hour,
  EXTRACT(DAYOFWEEK FROM start_date) AS day_of_week,

  -- nuevas features
  duration_min,
  distance_meters,

  -- label
  long_trip
FROM db_devfestgoogle.bike_sample_train;

  -- ============================================================
  -- IV. evaluate_bike_trip_model
  -- ============================================================

-- ======================================
-- EVALUACIÓN: TRAIN
-- ======================================
SELECT
  'TRAIN' AS dataset_name,
  *
FROM ML.EVALUATE(
  MODEL db_devfestgoogle.bike_trip_model,
  (
    SELECT
      start_station_id,
      end_station_id,
      subscriber_type,
      EXTRACT(HOUR FROM start_date) AS hour,
      EXTRACT(DAYOFWEEK FROM start_date) AS day_of_week,
      duration_min,
      distance_meters,
      long_trip
    FROM db_devfestgoogle.bike_sample_train
  )
)

UNION ALL

-- ======================================
-- EVALUACIÓN: TEST
-- ======================================
SELECT
  'TEST' AS dataset_name,
  *
FROM ML.EVALUATE(
  MODEL db_devfestgoogle.bike_trip_model,
  (
    SELECT
      start_station_id,
      end_station_id,
      subscriber_type,
      EXTRACT(HOUR FROM start_date) AS hour,
      EXTRACT(DAYOFWEEK FROM start_date) AS day_of_week,
      duration_min,
      distance_meters,
      long_trip
    FROM db_devfestgoogle.bike_sample_test
  )
)

UNION ALL

-- ======================================
-- EVALUACIÓN: BACKTEST
-- ======================================
SELECT
  'BACKTEST' AS dataset_name,
  *
FROM ML.EVALUATE(
  MODEL db_devfestgoogle.bike_trip_model,
  (
    SELECT
      start_station_id,
      end_station_id,
      subscriber_type,
      EXTRACT(HOUR FROM start_date) AS hour,
      EXTRACT(DAYOFWEEK FROM start_date) AS day_of_week,
      duration_min,
      distance_meters,
      long_trip
    FROM db_devfestgoogle.bike_sample_backtest
  )
);
  -- ============================================================
  -- V. predict_bike_trips
  -- ============================================================

CREATE OR REPLACE TABLE db_devfestgoogle.bike_trip_predictions AS
SELECT
  'TEST' AS dataset_name,
  start_date,
  start_station_id,
  end_station_id,
  subscriber_type,
  EXTRACT(HOUR FROM start_date) AS hour,
  EXTRACT(DAYOFWEEK FROM start_date) AS day_of_week,
  duration_min,
  distance_meters,
  long_trip,
  
  -- probabilidad de viaje largo (1)
  predicted_long_trip_probs[OFFSET(1)] AS prob_long_trip,

  -- clase predicha
  predicted_long_trip AS predicted_class

FROM ML.PREDICT(
  MODEL db_devfestgoogle.bike_trip_model,
  (
    SELECT
      start_date,
      start_station_id,
      end_station_id,
      subscriber_type,
      EXTRACT(HOUR FROM start_date) AS hour,
      EXTRACT(DAYOFWEEK FROM start_date) AS day_of_week,
      duration_min,
      distance_meters,
      long_trip
    FROM db_devfestgoogle.bike_sample_test
  )
)

UNION ALL

SELECT
  'BACKTEST' AS dataset_name,
  start_date,
  start_station_id,
  end_station_id,
  subscriber_type,
  EXTRACT(HOUR FROM start_date) AS hour,
  EXTRACT(DAYOFWEEK FROM start_date) AS day_of_week,
  duration_min,
  distance_meters,
  long_trip,

  predicted_long_trip_probs[OFFSET(1)] AS prob_long_trip,
  predicted_long_trip AS predicted_class

FROM ML.PREDICT(
  MODEL db_devfestgoogle.bike_trip_model,
  (
    SELECT
      start_date,
      start_station_id,
      end_station_id,
      subscriber_type,
      EXTRACT(HOUR FROM start_date) AS hour,
      EXTRACT(DAYOFWEEK FROM start_date) AS day_of_week,
      duration_min,
      distance_meters,
      long_trip
    FROM db_devfestgoogle.bike_sample_backtest
  )
);




