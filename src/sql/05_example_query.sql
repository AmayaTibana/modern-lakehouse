USE WAREHOUSE LAKEHOUSE_WH;
USE DATABASE LAKEHOUSE;
USE SCHEMA ANALYTICS;

-- Strongest matching Pokemon for each city's forecast period
SELECT l.location_name, f.period_name, f.short_forecast, wc.condition_group,
       p.name AS pokemon, t.type_name,
       p.hp + p.attack + p.defense + p.special_attack + p.special_defense + p.speed AS total_stats
FROM FACT_FORECAST f
JOIN DIM_LOCATION l ON f.location_key = l.location_key
JOIN DIM_WEATHER_CONDITION wc ON f.condition_key = wc.condition_key
JOIN BRIDGE_CONDITION_TYPE bct ON wc.condition_key = bct.condition_key
JOIN DIM_TYPE t ON bct.type_key = t.type_key
JOIN BRIDGE_POKEMON_TYPE bpt ON t.type_key = bpt.type_key
JOIN DIM_POKEMON p ON bpt.pokemon_id = p.pokemon_id
WHERE f.ingest_date = (SELECT MAX(ingest_date) FROM FACT_FORECAST)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY l.location_name, f.start_time ORDER BY total_stats DESC
) = 1
ORDER BY l.location_name, f.start_time;