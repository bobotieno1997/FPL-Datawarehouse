/*
=============================================================
Bronze Layer Table Creation
=============================================================
Script Purpose:
    This script creates the initial table structures in the 'bronze' schema of the 'fantasyPL' 
    database. The bronze layer serves as the landing zone for raw, unprocessed data ingested 
    from the Fantasy Premier League API. Four tables are created:
    - teams_info: Stores basic team information
    - players_info: Stores individual player details
    - games_info: Stores match/fixture information
    - player_stats: Stores player performance statistics

Assumptions:
    - The 'fantasyPL' database already exists
    - The 'bronze' schema already exists in the database
    - Data types are based on expected API response structures

WARNING:
    - If these tables already exist in the 'bronze' schema, running this script without 
      modification will result in an error. To recreate the tables, you would need to 
      explicitly DROP them first (not included here to prevent accidental data loss).
    - Ensure appropriate disk space is available for the database and schema.
    - No constraints (e.g., primary keys, foreign keys) are defined in this initial 
      bronze layer to maintain raw data integrity as received from the source.
*/

-- Create teams_info table for basic team details
CREATE TABLE silver.teams_info (
    team_id SMALLINT,
    team_code SMALLINT,
    team_name TEXT,
    team_short_name TEXT,
    logo_url TEXT,
    dwh_team_id BIGINT,
    dwh_ingestion_date TIMESTAMP DEFAULT NOW()
);

-- Create players_info table for individual player information
CREATE TABLE silver.players_info (
    player_id INT,
    first_name TEXT,
    second_name TEXT,
    web_name TEXT,
    team_code SMALLINT,
    team_id SMALLINT,
    player_position SMALLINT,
    player_code INT,
    player_region SMALLINT,
    can_select BOOL,
    photo_url TEXT,
    dwh_team_id BIGINT,
    dwh_player_id BIGINT,
    dwh_ingestion_date TIMESTAMP DEFAULT NOW()
);

-- Create games_info table for match/fixture details
CREATE TABLE silver.games_info (
game_code BIGINT,
game_week_id SMALLINT,
finished boolean,
game_id SMALLINT,
kickoff_time TIMESTAMP,
dwh_team_id_a BIGINT,
dwh_team_id_h BIGINT,
team_a_score SMALLINT,
team_h_score SMALLINT,
difficulty_a SMALLINT,
difficulty_h SMALLINT,
season INT,
dwh_ingestion_date TIMESTAMP DEFAULT NOW()
)

-- Create player_stats table for player performance statistics
CREATE TABLE silver.player_stats(
game_code BIGINT,
finished BOOLEAN,
game_id SMALLINT,
stats_value SMALLINT,
player_id SMALLINT,
team_type CHAR(1),
stat_type VARCHAR(30),
season INT,
dwh_player_id BIGINT,
dwh_ingestion_date TIMESTAMP DEFAULT NOW()
)

