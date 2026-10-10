USE WAREHOUSE LAKEHOUSE_WH;
USE DATABASE LAKEHOUSE;
USE SCHEMA ANALYTICS;

-- Dimensions
CREATE OR REPLACE TABLE DIM_DATE AS
SELECT TO_NUMBER(TO_CHAR(d, 'YYYYMMDD')) AS date_key,
       d AS full_date, YEAR(d) AS year, MONTH(d) AS month, DAY(d) AS day,
       DAYNAME(d) AS day_name
FROM (SELECT DATEADD(day, SEQ4(), '2026-01-01'::DATE) AS d
      FROM TABLE(GENERATOR(ROWCOUNT => 1500)));

CREATE OR REPLACE TABLE DIM_LOCATION AS
SELECT ROW_NUMBER() OVER (ORDER BY location) AS location_key,
       location AS location_name,
       MAX(latitude) AS latitude, MAX(longitude) AS longitude
FROM STAGING.STG_WEATHER_FORECASTS
GROUP BY location;

CREATE OR REPLACE TABLE DIM_POKEMON AS
SELECT id AS pokemon_id, name, height, weight, base_experience,
       hp, attack, defense, special_attack, special_defense, speed
FROM STAGING.STG_POKEMON;

CREATE OR REPLACE TABLE DIM_TYPE AS
SELECT ROW_NUMBER() OVER (ORDER BY type_name) AS type_key, type_name
FROM (SELECT DISTINCT type_name FROM STAGING.STG_POKEMON_TYPES);

CREATE OR REPLACE TABLE DIM_WEATHER_CONDITION (condition_key NUMBER, condition_group STRING);
INSERT INTO DIM_WEATHER_CONDITION VALUES
  (1,'SUNNY'),(2,'CLOUDY'),(3,'RAIN'),(4,'STORM'),(5,'SNOW'),(6,'WIND'),(7,'FOG'),(8,'OTHER');

-- Bridge tables
CREATE OR REPLACE TABLE BRIDGE_POKEMON_TYPE AS
SELECT s.pokemon_id, t.type_key, s.slot
FROM STAGING.STG_POKEMON_TYPES s
JOIN DIM_TYPE t ON s.type_name = t.type_name;

-- Weather-to-Pokemon-type mapping (your design decision, edit freely)
CREATE OR REPLACE TABLE BRIDGE_CONDITION_TYPE AS
SELECT c.condition_key, t.type_key
FROM (VALUES
  ('SUNNY','fire'),('SUNNY','grass'),
  ('CLOUDY','normal'),('CLOUDY','fairy'),
  ('RAIN','water'),('RAIN','electric'),
  ('STORM','electric'),('STORM','flying'),
  ('SNOW','ice'),
  ('WIND','flying'),
  ('FOG','ghost'),('FOG','psychic')
) AS m(condition_group, type_name)
JOIN DIM_WEATHER_CONDITION c ON c.condition_group = m.condition_group
JOIN DIM_TYPE t ON t.type_name = m.type_name;

-- Fact table
CREATE OR REPLACE TABLE FACT_FORECAST AS
WITH classified AS (
  SELECT w.*,
    CASE
      WHEN short_forecast ILIKE '%thunder%' THEN 'STORM'
      WHEN short_forecast ILIKE ANY ('%snow%','%flurr%','%sleet%') THEN 'SNOW'
      WHEN short_forecast ILIKE ANY ('%rain%','%shower%','%drizzle%') THEN 'RAIN'
      WHEN short_forecast ILIKE '%fog%' THEN 'FOG'
      WHEN short_forecast ILIKE ANY ('%windy%','%breezy%') THEN 'WIND'
      WHEN short_forecast ILIKE ANY ('%sunny%','%clear%') THEN 'SUNNY'
      WHEN short_forecast ILIKE '%cloud%' THEN 'CLOUDY'
      ELSE 'OTHER'
    END AS condition_group
  FROM STAGING.STG_WEATHER_FORECASTS w
)
SELECT l.location_key,
       TO_NUMBER(TO_CHAR(c.start_time::DATE, 'YYYYMMDD')) AS date_key,
       wc.condition_key,
       c.ingest_date, c.period_number, c.period_name, c.start_time,
       c.temperature, c.temperature_unit, c.is_daytime,
       c.wind_speed, c.short_forecast
FROM classified c
JOIN DIM_LOCATION l ON c.location = l.location_name
JOIN DIM_WEATHER_CONDITION wc ON c.condition_group = wc.condition_group;