-- Creates or replaces a view in the gold layer that provides dimensional data for teams
-- Purpose: Serves as a standardized, unique-keyed reference table for team information
-- Source: silver.teams_info table

CREATE OR REPLACE VIEW gold.v_dimension_teams AS
SELECT DISTINCT 
    -- Generates a unique surrogate key for each team using ROW_NUMBER
    -- Ordered by team_code for consistent key assignment
    ROW_NUMBER() OVER (ORDER BY team_code ASC) AS team_key,
    
    -- Primary team identifier from source system
    team_code,
    
    -- Full official name of the team
    team_name,
    
    -- Abbreviated team name for compact displays/reports
    team_short_name,
    
    -- URL reference to team's logo image
    -- Renamed for clarity and consistency with dimension naming
    logo_url AS team_logo
FROM 
    silver.teams_info
-- Ensures consistent ordering in the output for reliability
ORDER BY 
    team_code ASC;