/*
    Stored Procedure: Update Silver Layer Teams Info
    Purpose: Incrementally update silver.teams_info with new records from bronze, i.e team_codes that dont exist in the silver layer table are added
    Source: bronze.teams_info
    Target: silver.teams_info
*/


-- Create or replace the stored procedure
CREATE OR REPLACE PROCEDURE silver.usp_update_team_info()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '=======================================';
    RAISE NOTICE 'Update silver.teams_info table';
    RAISE NOTICE '=======================================';
    
        INSERT INTO silver.teams_info(
        team_id,             --- Team id for the team in that season, this changes every season as new teams join and leave the season
        team_code,           --- Unique code for each team to ever participate in Premier league
        team_name,           --- FUll team name
        team_short_name,     ---short name for the team
        logo_url,            ---Logo url of the team
        dwh_team_id          ---unique code generated from combining team_id and season.
        )
        SELECT 
            CAST(bti.team_id AS SMALLINT)AS team_id,
            CAST(bti.team_code AS SMALLINT) AS team_code,
            bti.team_name,
            bti.team_short_name,
            bti.logo_url,
            CAST(
            CONCAT(bti.team_id,
            EXTRACT(YEAR FROM bti.min_kickoff::TIMESTAMP),
            EXTRACT(YEAR FROM bti.max_kickoff::TIMESTAMP)) AS BIGINT)AS dwh_team_id
        FROM bronze.teams_info bti 
        LEFT JOIN silver.teams_info sti ON sti.dwh_team_id = CAST(CONCAT(bti.team_id,
        EXTRACT(YEAR FROM bti.min_kickoff::TIMESTAMP),
        EXTRACT(YEAR FROM bti.max_kickoff::TIMESTAMP)) AS BIGINT)
        WHERE sti.dwh_team_id IS NULL;

    -- Success message
    RAISE NOTICE '=======================================';
    RAISE NOTICE 'Updated silver.teams_info table successfully';
    RAISE NOTICE '=======================================';

EXCEPTION
    WHEN OTHERS THEN
        -- Error handling: Log the error message
        RAISE NOTICE 'ERROR: %', SQLERRM;
        RAISE;  -- Re-raise the exception after logging
END;
$$