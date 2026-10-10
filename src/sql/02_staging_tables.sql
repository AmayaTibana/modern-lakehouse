USE WAREHOUSE LAKEHOUSE_WH;
USE DATABASE LAKEHOUSE;
USE SCHEMA STAGING;

CREATE TABLE IF NOT EXISTS STG_POKEMON (
  id NUMBER, name STRING, height NUMBER, weight NUMBER, base_experience NUMBER,
  hp NUMBER, attack NUMBER, defense NUMBER,
  special_attack NUMBER, special_defense NUMBER, speed NUMBER
);

CREATE TABLE IF NOT EXISTS STG_POKEMON_TYPES (
  pokemon_id NUMBER, slot NUMBER, type_name STRING
);

CREATE TABLE IF NOT EXISTS STG_WEATHER_FORECASTS (
  location STRING, latitude FLOAT, longitude FLOAT, ingested_at STRING,
  ingest_date DATE, period_number NUMBER, period_name STRING,
  start_time TIMESTAMP_NTZ, end_time TIMESTAMP_NTZ,
  temperature NUMBER, temperature_unit STRING, is_daytime BOOLEAN,
  wind_speed STRING, wind_direction STRING,
  short_forecast STRING, detailed_forecast STRING
);