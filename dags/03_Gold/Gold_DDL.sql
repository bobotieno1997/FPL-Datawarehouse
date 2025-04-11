/*
 =============================================================
 Gold Layer Table Creation
 =============================================================
 Script Purpose:
 This script creates the dimension and fact table structures in the 'gold' layer of the 
 Fantasy Premier League (FPL) data warehouse. This layer is designed for clean, 
 structured, and query-optimized data used for reporting and analytics.
 
 The schema includes:
 - Dimensional tables for teams, players, and stat types
 - Fact tables for player history, team standings, match results, player stats, and upcoming games
 
 Assumptions:
 - The database already exists and is accessible
 - The 'gold' schema is being used or referenced implicitly
 - Keys are managed and either populated by source systems or automatically incremented
 
 Conventions:
 - Primary keys are defined on all tables to support analytics engine indexing
 - Data types are optimized for performance and consistency
 - Naming follows Dim (dimension) and Fct (fact) conventions for clarity
 
 WARNING:
 - These statements use `CREATE TABLE IF NOT EXISTS`, so re-running them won’t overwrite existing tables
 - To recreate or alter table structures, you will need to drop them explicitly
 - Ensure foreign keys are respected in downstream models if added later
 */
-- =============================================================
-- Dimension Tables
-- =============================================================
-- Stores master data for all FPL teams
CREATE TABLE IF NOT EXISTS DimTeams (
    team_key INT PRIMARY KEY,
    team_code INT,
    team_name TEXT NOT NULL,
    team_short_name CHAR(4),
    team_logo TEXT
);
-- Stores master data for all FPL players
CREATE TABLE IF NOT EXISTS DimPlayers (
    player_key INT PRIMARY KEY,
    player_code INT,
    first_name TEXT NOT NULL,
    second_name TEXT NOT NULL,
    web_name TEXT NOT NULL,
    player_photo TEXT
);
-- Stores types of statistics tracked per player in games (e.g., goals, assists, saves)
CREATE TABLE IF NOT EXISTS DimStatType (
    stat_key INT PRIMARY KEY,
    stat_type VARCHAR(30)
);
-- =============================================================
-- Fact Tables
-- =============================================================
-- Stores historical metadata for players including position, region, and team over seasons
CREATE TABLE IF NOT EXISTS FctPlayerHistory (
    player_hsitory_key INT PRIMARY KEY,
    player_code VARCHAR(30),
    player_position INT,
    player_region INT,
    team_code INT,
    season INT
);
-- Stores team-level match outcomes, including goals, position, and points per fixture
CREATE TABLE IF NOT EXISTS FctStanding (
    standing_key INT PRIMARY KEY AUTO_INCREMENT,
    game_code INT,
    game_week_id INT,
    game_id INT,
    team_code INT,
    goals_for INT,
    goals_against INT,
    home_away TEXT,
    game_status TEXT,
    points INT,
    cumm_points INT,
    position INT,
    season INT
);
-- Stores results of matches including kickoff times, teams, scores, and match difficulty
CREATE TABLE IF NOT EXISTS FctResults (
    result_key INT PRIMARY KEY AUTO_INCREMENT,
    game_code INT,
    game_week_id INT,
    kickoff_time TIMESTAMP,
    home_team_code INT,
    game_score TEXT,
    away_team_code INT,
    difficulty_h INT,
    difficulty_a INT,
    season INT
);
-- Stores player-level stats captured per match (e.g., 1 goal, 2 assists)
CREATE TABLE IF NOT EXISTS FctPlayerStats (
    stat_key INT PRIMARY KEY AUTO_INCREMENT,
    game_code INT,
    game_id INT,
    value INT,
    player_code INT,
    stats_key INT,
    season INT
);
-- Stores scheduled future matches with expected teams and difficulty
CREATE TABLE IF NOT EXISTS FctFutureGames (
    game_key INT PRIMARY KEY AUTO_INCREMENT,
    game_code INT,
    game_week_id INT,
    home_team_code INT,
    kickoff_time TIMESTAMP,
    away_team_code INT,
    difficulty_h INT,
    difficulty_a INT,
    season INT
);