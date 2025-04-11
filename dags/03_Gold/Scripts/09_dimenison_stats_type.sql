-- Creates or replaces a view in the gold schema for player statistics facts with dimensional references
-- Purpose: Stores for each stat type available
-- Source: silver.player_stats
CREATE OR REPLACE VIEW gold.v_dimension_stats
AS
create or replace view gold.v_dimension_stat_type
AS
WITH unique_stats AS (
    SELECT DISTINCT stat_type
    FROM silver.player_stats
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY stat_type ASC) AS stat_key,
    stat_type
FROM unique_stats;