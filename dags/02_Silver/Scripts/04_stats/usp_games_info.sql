/*
    Stored Procedure: silver.usp_update_players_stats
    Purpose: Incrementally updates silver.player_stats with new records from bronze.player_stats,
             preserving historical data by inserting only rows not already present.
    Source: bronze.player_stats
    Target: silver.player_stats
*/

CREATE OR REPLACE PROCEDURE silver.usp_update_players_stats()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Log start of the procedure
    RAISE NOTICE '=====================================';
    RAISE NOTICE 'Updating silver.player_stats';
    RAISE NOTICE '=====================================';

    -- Insert new player stats records from bronze layer into silver layer
    INSERT INTO silver.player_stats (
        game_code,
        finished,
        game_id,
        stats_value,
        player_id,
        team_type,
        stat_type,
        season,
        dwh_player_id
    )
    SELECT 
        bps.game_code,
        bps.finished,
        bps.game_id,
        bps.stat_value,
        bps.player_id,
        bps.team_type,
        bps.stat_type,
        bps.season,
        CAST(CONCAT(bps.player_id, bps.season) AS BIGINT) AS dwh_player_id
    FROM (
        -- Subquery to derive season based on game_code ranges
        SELECT 
            *,
            CAST(
                CASE 
                    WHEN game_code BETWEEN 2444470 AND 2444849 THEN '20242025'
                    -- Add new CASE branches here for future seasons as API data expands
                    ELSE '20252026' -- Default for future seasons until updated
                END AS INTEGER) AS season
        FROM bronze.player_stats
    ) bps
    LEFT JOIN silver.player_stats sps 
        ON bps.game_code = sps.game_code
    WHERE sps.game_code IS NULL; -- Ensures only new records are inserted

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors and re-raise to propagate the exception
        RAISE NOTICE '=====================================';
        RAISE NOTICE 'ERROR MESSAGE: %', SQLERRM;
        RAISE NOTICE '=====================================';
        RAISE;
END;
$$;