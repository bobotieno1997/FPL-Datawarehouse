-- Create or replace the view for detailed game results
-- Purpose: Stores detailed game-by-game results with home and away team information
-- Source: silver.teams_info, silver.games_info

CREATE OR REPLACE VIEW gold.v_fct_results AS
SELECT 
    sgi.game_code,                     -- Unique identifier for each game
    sgi.game_week_id,                  -- Week number within the season
    sgi.kickoff_time,
    sti.team_code AS home_team_code,   -- Home team's code from teams_info
    CONCAT(sgi.team_h_score, '-', sgi.team_a_score) AS game_score,  -- Score in format "home-away"
    (SELECT sti2.team_code 
     FROM silver.teams_info sti2 
     WHERE sti2.dwh_team_id = sgi.dwh_team_id_a) AS away_team_code,  -- Away team's code via subquery
    sgi.difficulty_h,                  -- Home team difficulty metric (e.g., opponent strength)
    sgi.difficulty_a,                  -- Away team difficulty metric (e.g., opponent strength)
    sgi.season                         -- Season identifier (e.g., year or season name)
FROM silver.games_info sgi 
LEFT JOIN silver.teams_info sti 
    ON sgi.dwh_team_id_h = sti.dwh_team_id  -- Left join to include all games, even if home team data is missing
ORDER BY sgi.game_code, sgi.game_week_id ASC;  -- Order by game_code and game_week_id for consistent chronological output