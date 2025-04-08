/*
    Data Quality Monitoring Script for Power BI Integration
    Purpose: Validate data integrity in bronze.players_info table
    Note: Results will feed into Power BI notifications for monitoring
    Date: March 04, 2025
*/

-- Check #1: Validate integer/numeric fields for unexpected NULL values
-- Note: Region column is allowed to have NULL values as per business rules
-- All other fields should contain valid values
SELECT DISTINCT
    player_id,
    team_code,
    team_id,
    player_position,
    player_code,
    region
FROM bronze.players_info pi2
WHERE player_id IS NULL 
   OR team_code IS NULL 
   OR team_id IS NULL 
   OR player_position IS NULL 
   OR region IS NULL;

-- Check #2: Validate text fields for unwanted leading/trailing spaces
-- Purpose: Ensure consistent text formatting
-- Expectation: All text values should be properly trimmed
SELECT 
    first_name,
    second_name,
    web_name
FROM bronze.players_info pi2
WHERE first_name <> TRIM(first_name) 
   OR second_name <> TRIM(second_name) 
   OR web_name <> TRIM(web_name);

-- Check #3: Validate text fields for NULL values
-- Business Rule: No NULL values permitted in name fields
-- Action: Flag any records with missing name information
SELECT DISTINCT
    first_name,
    second_name,
    web_name
FROM bronze.players_info pi2
WHERE first_name IS NULL 
   OR second_name IS NULL 
   OR web_name IS NULL;

   