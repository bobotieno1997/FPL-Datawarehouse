/*
    Test Script: Data Quality Checks for silver.players_info - Duplicates
    Purpose: Detects duplicate player records across all fields to ensure data integrity.
    Table: silver.players_info
    Expected Result: Empty result set indicates no duplicates; non-empty set flags redundancy.
    Action: Investigate ETL deduplication logic if duplicates are found.
*/
SELECT
    player_id,
    first_name,
    second_name,
    web_name,
    team_code,
    team_id,
    player_position,
    player_code,
    player_region,
    can_select,
    photo_url,
    COUNT(*) AS duplicate_count
FROM silver.players_info
GROUP BY 
    player_id,
    first_name,
    second_name,
    web_name,
    team_code,
    team_id,
    player_position,
    player_code,
    player_region,
    can_select,
    photo_url
HAVING COUNT(*) > 1;

/*
    Test Script: Data Quality Checks for silver.games_info - Duplicate Game Codes
    Purpose: Identifies duplicate game_code values to confirm unique game entries.
    Table: silver.games_info
    Expected Result: Empty result set indicates no duplicate game codes; non-empty set requires review.
    Action: Check source data or transformation process if duplicates appear.
*/
SELECT DISTINCT 
    game_code,
    COUNT(game_code) AS game_code_count
FROM silver.games_info
GROUP BY game_code
HAVING COUNT(game_code) > 1;