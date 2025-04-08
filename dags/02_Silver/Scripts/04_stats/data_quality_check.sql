/*
    Test Script: Data Quality Check for silver.player_stats - Null Values
    Purpose: Detects rows with null values in critical fields to verify data completeness.
    Table: silver.player_stats
    Note: Duplicates are expected in this table and are not evaluated here.
    Expected Result: Empty result set indicates no nulls; non-empty set flags incomplete data.
    Action: Investigate source data or ETL process if nulls are found in key fields.
*/
SELECT
    game_code,
    finished,
    game_id,
    stats_value,
    player_id,
    team_type,
    stat_type,
    season,
    dwh_player_id,
    dwh_ingestion_date
FROM silver.player_stats
WHERE 
    game_code IS NULL 
    OR finished IS NULL 
    OR game_id IS NULL 
    OR stats_value IS NULL 
    OR player_id IS NULL 
    OR team_type IS NULL;