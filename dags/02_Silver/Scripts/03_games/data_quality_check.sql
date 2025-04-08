/*
    Test Script: Data Quality Check for silver.games_info - Duplicate Game Codes
    Purpose: Identifies duplicate game_code values as a representative check for table uniqueness.
    Table: silver.games_info
    Expected Result: Empty result set indicates unique game codes; non-empty set flags duplicates.
    Action: Review source data or ETL deduplication if duplicates are detected.
*/
SELECT DISTINCT 
    game_code,
    COUNT(game_code) AS game_code_count
FROM silver.games_info
GROUP BY game_code
HAVING COUNT(game_code) > 1;

/*
    Test Script: Data Quality Check for silver.games_info - Null Values
    Purpose: Detects rows with null values in critical fields to verify data completeness.
    Table: silver.games_info
    Expected Result: Empty result set indicates no nulls; non-empty set requires investigation.
    Action: Check upstream data source or transformation logic if nulls are found.
*/
SELECT
    game_code,
    game_week_id,
    finished,
    game_id,
    kickoff_time
FROM silver.games_info
WHERE 
    game_code IS NULL 
    OR game_week_id IS NULL 
    OR finished IS NULL 
    OR game_id IS NULL 
    OR kickoff_time IS NULL;