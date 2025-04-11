-- Creates or replaces a view in the gold schema for seasons data we have in the database
-- Purpose: Stores lookup data for all the season data we have in the database
-- Source: silver.games_info

CREATE OR REPLACE VIEW gold_v_dimension_season
AS
SELECT 
    DISTINCT
    CONCAT(
        LEFT(season::text,4), '/', RIGHT(season::text,4)) as season_name,
    season 
FROM silver.games_info
ORDER BY season ASC;