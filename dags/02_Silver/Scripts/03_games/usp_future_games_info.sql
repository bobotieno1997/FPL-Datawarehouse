/*
    Stored Procedure: silver.usp_update_future_games_info
    Purpose: Clears and updates the silver.future_games_info table with remaining games of the current season.
             Data is truncated because only one season's games are available from the source at a time.
    Source: bronze.games_info
    Target: silver.future_games_info
*/

-- Create or replace the stored procedure
CREATE OR REPLACE PROCEDURE silver.usp_update_future_games_info()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Log procedure start
    RAISE NOTICE '=====================================';
    RAISE NOTICE 'Starting update of silver.future_games_info at %', CURRENT_TIMESTAMP;
    RAISE NOTICE '=====================================';

    -- Check table existence before truncation
    IF EXISTS (
        SELECT FROM pg_tables 
        WHERE schemaname = 'silver' 
        AND tablename = 'future_games_info'
    ) THEN
        -- Truncate table to remove old data, as only current season is relevant
        TRUNCATE TABLE silver.future_games_info;
        RAISE NOTICE 'Table silver.future_games_info truncated successfully';
    ELSE
        RAISE NOTICE 'Table silver.future_games_info does not exist, skipping truncate';
    END IF;

    -- Insert new game records into the silver layer
    INSERT INTO silver.future_games_info(
        game_code,
        game_week_id,
        finished,
        game_id,
        kickoff_time,
        dwh_team_id_a,
        dwh_team_id_h,
        difficulty_a,
        difficulty_h,
        season
    )
    SELECT 
        bgi.game_code,                        -- Unique game identifier
        bgi.game_week_id,                     -- Game week identifier
        bgi.finished,                         -- Game completion status
        bgi.game_id,                          -- Primary game ID
        bgi.kickoff_time,                     -- Scheduled start time
        CAST(CONCAT(bgi.team_id_a, bgi.season) AS BIGINT) AS dwh_team_id_a,  -- Concatenated away team ID with season
        CAST(CONCAT(bgi.team_id_h, bgi.season) AS BIGINT) AS dwh_team_id_h,  -- Concatenated home team ID with season
        bgi.difficulty_a,                     -- Difficulty for away team
        bgi.difficulty_h,                     -- Difficulty for home team
        bgi.season                            -- Calculated season
    FROM (
        -- Subquery to calculate season based on game_code ranges
        SELECT 
            *,
            CAST(
                CASE 
                    WHEN game_code BETWEEN 2444470 AND 2444849 THEN '20242025'  -- Current season range
                    WHEN game_code BETWEEN 2444850 AND 2445229 THEN '20252026'  -- Next season (example)
                    ELSE '99999999'  -- Default for unhandled ranges, flags issues
                END AS INTEGER) AS season
        FROM bronze.games_info
        WHERE finished = FALSE  -- Filter for remaining (future) games
    ) bgi;

    -- Log successful insertion
    RAISE NOTICE 'Inserted % rows into silver.future_games_info', (SELECT COUNT(*) FROM silver.future_games_info);
    RAISE NOTICE 'Update completed successfully at %', CURRENT_TIMESTAMP;

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors encountered during execution
        RAISE NOTICE '=====================================';
        RAISE NOTICE 'Error occurred in silver.usp_update_future_games_info: %', SQLERRM;
        RAISE NOTICE '=====================================';
        RAISE; -- Re-raise exception to ensure caller (e.g., Airflow) handles failure
END;
$$;