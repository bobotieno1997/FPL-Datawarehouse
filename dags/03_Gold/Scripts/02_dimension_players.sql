-- Creates or replaces a view in the gold layer providing dimensional data for players
-- Purpose: Serves as a standardized, unique-keyed reference table for player information
-- Source: silver.players_info table
CREATE OR REPLACE VIEW gold.v_dimension_players AS
SELECT player_key,
    player_code,
    first_name,
    second_name,
    web_name,
    player_photo
FROM (
        SELECT DISTINCT -- Generates a unique surrogate key for each player using ROW_NUMBER()
            -- Ordered by player_code for deterministic key assignment
            ROW_NUMBER() OVER (
                ORDER BY player_code ASC
            ) AS player_key,
            -- Primary player identifier from the source system
            player_code,
            -- Player's given first name
            first_name,
            -- Player's surname or family name
            second_name,
            -- Player's display name used in web interfaces
            web_name,
            -- Added missing comma here
            -- URL reference to the player's photo
            -- Renamed to player_photo for clarity and naming consistency
            photo_url AS player_photo,
            COUNT(1) OVER(
                PARTITION BY player_code,
                first_name,
                second_name,
                web_name,
                photo_url
                ORDER BY dwh_ingestion_date DESC
            ) counts
        FROM silver.players_info
    ) tbl
WHERE tbl.counts = 1;