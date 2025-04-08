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
CREATE TABLE bronze.teams_info (
    team_id SMALLINT,
    team_code SMALLINT,
    team_name VARCHAR(20),
    team_short_name CHAR(3)
);

-- Create players_info table for individual player information
CREATE TABLE bronze.players_info (
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
    photo_url TEXT
);

-- Create games_info table for match/fixture details
CREATE TABLE bronze.games_info (
    gamecode INT,
    game_week_id SMALLINT,
    finished BOOL,
    game_id SMALLINT,
    kickoff_time SMALLINT,
    team_id_h SMALLINT,
    team_id_a SMALLINT,
    team_h_score INT,
    team_a_score INT,
    difficuly_h SMALLINT,
    difficuly_a SMALLINT
);

-- Create a table for future games alone
CREATE TABLE silver.future_games_info(
    game_code BIGINT,
    game_week_id SMALLINT,
    finished BOOLEAN,
    game_id SMALLINT,
    kickoff_time TIMESTAMP,
    dwh_team_id_a BIGINT,
    dwh_team_id_h BIGINT,
    difficulty_a SMALLINT,
    difficulty_h SMALLINT,
season INT
)

-- Create player_stats table for player performance statistics
CREATE TABLE bronze.player_stats (
    game_code INT,
    finished BOOL,
    game_id SMALLINT,
    stat_value SMALLINT,
    player_id SMALLINT,
    team_type CHAR(1),
    stat_type VARCHAR(20)
);
