-- Creates or replaces a view in the gold layer providing historical facts for players
-- Purpose: Serves as a fact table tracking player history with a unique identifier per record
-- Source: silver.players_info table

CREATE OR REPLACE VIEW gold.v_fct_player_history AS
SELECT 
    -- Generates a unique surrogate key for each history record using ROW_NUMBER()
    -- Ordered by player_code for deterministic key assignment
    ROW_NUMBER() OVER (ORDER BY player_code ASC) AS history_id,
    
    -- Primary player identifier from the source system
    player_code,
    
    -- Player's position on the team (e.g., forward, defender)
    player_position,
    
    -- Geographic region associated with the player
    player_region,  -- Added missing comma here
    
    -- Extracts the last 8 characters from dwh_team_id as the season identifier
    -- Casts to integer after extracting from text representation
    CAST(RIGHT(CAST(dwh_team_id AS TEXT), 8) AS INT) AS season
FROM 
    silver.players_info spi  -- Alias 'spi' for clarity and brevity
-- Orders the underlying data by player_code for consistent key generation
-- Note: Query this view with ORDER BY if specific ordering is needed
ORDER BY 
    player_code ,dwh_ingestion_date ASC