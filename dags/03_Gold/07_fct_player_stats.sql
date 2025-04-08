-- Creates or replaces a view in the gold schema for player statistics facts with dimensional references
-- Purpose: Stores for stats for each player in given gameweek
-- Source: silver.player_info, silver.player_stats, gold.v_dimension_stats_type

CREATE OR REPLACE VIEW gold.v_fct_player_stats
AS
SELECT 
    ps.game_code,           -- Unique code identifying the game
    ps.game_id,             -- Numeric identifier for the game
    ps.stats_value AS value,-- Recorded value of the statistic
    spi.player_code,        -- Player's unique code from player info
    ps.season,              -- Season during which the stats were recorded
    gs.stats_key AS stats_key    -- Foreign key linking to stat type dimension
FROM silver.player_stats ps         -- Source table containing player statistics
    -- Left join to include all player stats even if stat type is missing
    LEFT JOIN gold.v_dimension_stat_type gs 
        ON gs.stat_type = ps.stat_type  -- Links stats to stat type dimension
    -- Inner join to ensure player information exists
    JOIN silver.players_info spi 
        ON spi.dwh_player_id = ps.dwh_player_id  -- Links stats to player info
-- Orders the results by game and player
ORDER BY ps.game_code, spi.player_code ASC;