/*
    Test Script: Data Quality Checks for silver.teams_info - Null Values
    Purpose: Identifies rows with null values in critical fields to ensure data completeness.
    Table: silver.teams_info
    Expected Result: Empty result set indicates no nulls; non-empty set flags data issues.
    Action: Investigate source data or ETL process if nulls are found.
*/
SELECT DISTINCT
    team_id,
    team_code,
    team_name,
    team_short_name,
    dwh_team_id
FROM silver.teams_info 
WHERE 
    team_id IS NULL 
    OR team_code IS NULL 
    OR team_name IS NULL 
    OR team_short_name IS NULL 
    OR dwh_team_id IS NULL;

/*
    Test Script: Data Quality Checks for silver.teams_info - Duplicates
    Purpose: Detects duplicate rows across all fields to verify data uniqueness.
    Table: silver.teams_info
    Expected Result: Empty result set indicates no duplicates; non-empty set indicates redundancy.
    Action: Review deduplication logic in ETL if duplicates are detected.
*/
SELECT
    team_id,
    team_code,
    team_name,
    team_short_name,
    dwh_team_id,
    COUNT(*) AS duplicate_count
FROM silver.teams_info
GROUP BY 
    team_id,
    team_code,
    team_name,
    team_short_name,
    dwh_team_id
HAVING COUNT(*) > 1;

/*
    Test Script: Data Quality Checks for silver.players_info - Null Values
    Purpose: Identifies rows with null values in key fields to ensure complete player records.
    Table: silver.players_info
    Expected Result: Empty result set indicates no nulls; non-empty set requires investigation.
    Action: Check source data or transformation steps if nulls appear.
*/
SELECT DISTINCT
    player_id,
    first_name,
    second_name,
    web_name,
    team_code,
    team_id,
    player_position,
    player_code
FROM silver.players_info
WHERE 
    player_id IS NULL 
    OR first_name IS NULL 
    OR second_name IS NULL 
    OR web_name IS NULL 
    OR team_code IS NULL 
    OR team_id IS NULL 
    OR player_position IS NULL 
    OR player_code IS NULL;