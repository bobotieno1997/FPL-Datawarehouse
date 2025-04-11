-- Creates or replaces a view in the gold layer providing historical data for team standing at given gameweek
-- Purpose: Serves as a fact table tracking team standing through out the season, used for team position trend analysis
-- Source: silver.teams_info table

CREATE OR REPLACE VIEW gold.v_fct_standing AS
SELECT
    -- Game Identification Metrics
    game_code,              -- Unique identifier for each game
    game_week_id,           -- Week number within the season
    game_id,                -- Internal game identifier
    -- Team Information
    sti.team_code,          -- Team's unique code from teams_info table
    -- Performance Metrics
    goals_for,              -- Goals scored by the team in this game
    goals_against,          -- Goals conceded by the team in this game
    -- Game Context
    home_away,              -- 'H' for home, 'A' for away
    game_status,            -- 'W' for win, 'L' for loss, 'D' for draw
    points,                 -- Earned points for each team in each game
	cumm_points,            -- Cumulative points up to this game week for the team
    -- Team ranking within each season and game week based on points
    RANK() OVER (PARTITION BY ar.season, ar.game_week_id ORDER BY points DESC) AS position,
	season	             -- Season identifier (e.g., year or season name)
FROM (
    -- Subquery to calculate game results and cumulative points per team
    SELECT
        game_code,
        game_week_id,
        game_id,
        dwh_team_id,        -- Data warehouse team identifier
        goals_for,
        goals_against,
        home_away,
        game_status,
        season,
        -- Calculate running total of points: 3 for win, 1 for draw, 0 for loss
        SUM(CASE WHEN game_status = 'W' THEN 3
                 WHEN game_status = 'L' THEN 0
                 ELSE 1 END) OVER (PARTITION BY dwh_team_id, season ORDER BY game_week_id) AS cumm_points,
		CASE WHEN game_status = 'W' THEN 3
                 WHEN game_status = 'L' THEN 0
                 ELSE 1 END AS points
    FROM (
        -- Winning teams: select winners and their scores from non-drawn games
        SELECT
            game_code,
            game_week_id,
            game_id,
            CASE WHEN team_h_score > team_a_score THEN dwh_team_id_h
                 ELSE dwh_team_id_a END AS dwh_team_id,    -- Winner's team ID
            CASE WHEN team_h_score > team_a_score THEN team_h_score
                 ELSE team_a_score END AS goals_for,       -- Winner's goals
            CASE WHEN team_h_score > team_a_score THEN team_a_score
                 ELSE team_h_score END AS goals_against,   -- Loser's goals
            CASE WHEN team_h_score > team_a_score THEN 'H'
                 ELSE 'A' END AS home_away,                -- Winner's location
            'W' AS game_status,                            -- Mark as win
            season
        FROM silver.games_info
        WHERE team_h_score <> team_a_score                 -- Exclude draws
        
        UNION ALL
        
        -- Losing teams: select losers and their scores from non-drawn games
        SELECT
            game_code,
            game_week_id,
            game_id,
            CASE WHEN team_h_score > team_a_score THEN dwh_team_id_a
                 ELSE dwh_team_id_h END AS dwh_team_id,    -- Loser's team ID
            CASE WHEN team_h_score > team_a_score THEN team_a_score
                 ELSE team_h_score END AS goals_for,       -- Loser's goals
            CASE WHEN team_h_score > team_a_score THEN team_h_score
                 ELSE team_a_score END AS goals_against,   -- Winner's goals
            CASE WHEN team_h_score > team_a_score THEN 'A'
                 ELSE 'H' END AS home_away,                -- Loser's location
            'L' AS game_status,                            -- Mark as loss
            season
        FROM silver.games_info
        WHERE team_h_score <> team_a_score                 -- Exclude draws

        UNION ALL

        -- Drawn games: combine home and away teams from tied games
        SELECT
            game_code,
            game_week_id,
            game_id,
            dwh_team_id,
            goals_for,
            goals_against,
            home_away,
            'D' AS game_status,                            -- Mark as draw
            season
        FROM (
            -- Home teams in draws
            SELECT
                game_code,
                game_week_id,
                game_id,
                dwh_team_id_h AS dwh_team_id,             -- Home team ID
                team_h_score AS goals_for,                -- Home team goals
                team_a_score AS goals_against,            -- Away team goals
                'H' AS home_away,                         -- Home indicator
                season
            FROM silver.games_info
            WHERE team_h_score = team_a_score             -- Only draws
            UNION ALL
            -- Away teams in draws
            SELECT
                game_code,
                game_week_id,
                game_id,
                dwh_team_id_a,                            -- Away team ID
                team_a_score,                             -- Away team goals
                team_h_score,                             -- Home team goals
                'A',                                      -- Away indicator
                season
            FROM silver.games_info
            WHERE team_h_score = team_a_score             -- Only draws
        ) draws
    ) results  -- Alias for combined game results
) ar  -- Alias for results with points calculation
-- Join with team info to get team_code
JOIN silver.teams_info sti ON ar.dwh_team_id = sti.dwh_team_id
-- Order results chronologically by season, game week, and game code for consistent output
ORDER BY season, game_week_id, game_code ASC;