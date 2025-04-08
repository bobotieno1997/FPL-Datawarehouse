/*
    Stored Procedure: silver.usp_update_games_info
    Purpose: Inserts new game records from bronze.games_info into silver.games_info,
             ensuring historical data is preserved by only adding non-existing entries.
    Source: bronze.games_info
    Target: silver.games_info
*/

-- Create or replace the stored procedure
CREATE OR REPLACE PROCEDURE silver.usp_update_games_info()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Log procedure start
    RAISE NOTICE '=====================================';
    RAISE NOTICE 'UPDATE TABLE silver.games_info';
    RAISE NOTICE '=====================================';

    -- Insert new game records into the silver layer
    INSERT INTO silver.games_info (
        game_code, game_week_id, finished, game_id, kickoff_time,
        dwh_team_id_a, dwh_team_id_h, team_a_score, team_h_score,
        difficulty_a, difficulty_h, season
    )
    SELECT 
        bgi.game_code,
        bgi.game_week_id,
        bgi.finished,
        bgi.game_id,
        bgi.kickoff_time,
        CAST(CONCAT(bgi.team_id_a, bgi.season) AS BIGINT) AS dwh_team_id_a,
        CAST(CONCAT(bgi.team_id_h, bgi.season) AS BIGINT) AS dwh_team_id_h,
        bgi.team_a_score,
        bgi.team_h_score,
        bgi.difficulty_a,
        bgi.difficulty_h,
        bgi.season
    FROM (
        -- Subquery to calculate season based on game_code
        SELECT 
            *,
            CAST(
                CASE 
                    WHEN game_code BETWEEN 2444470 AND 2444849 THEN '20242025'
                    -- Add new CASE branches here for future seasons as API data evolves
                    ELSE '20252026' -- Placeholder for future seasons
                END AS INTEGER) AS season
        FROM bronze.games_info
		WHERE finished = TRUE
    ) bgi
    LEFT JOIN silver.games_info sgi 
        ON bgi.game_code = sgi.game_code
    WHERE sgi.game_code IS NULL -- Filters to only new records
	ORDER BY bgi.game_code, bgi.season, bgi.game_week_id;

EXCEPTION
    WHEN OTHERS THEN
        -- Log errors and propagate exception to caller
        RAISE NOTICE '=====================================';
        RAISE NOTICE 'ERROR MESSAGE: %', SQLERRM;
        RAISE NOTICE '=====================================';
        RAISE; -- Ensures errors are not silently ignored
    
END;
$$;