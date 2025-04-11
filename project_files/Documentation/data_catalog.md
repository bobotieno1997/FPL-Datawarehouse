# Data Catalogue for Gold Reporting Layer

This document catalogs the views in the `gold` schema, designed for reporting and analytics. Each view is detailed with its purpose, structure, and source, providing a standardized reference for downstream consumption.

---

## View: `gold.v_dimension_teams`

### Description
A dimensional view providing standardized, unique-keyed reference data for teams.

### Purpose
Serves as a reliable lookup table for team information with consistent surrogate keys.

### Source
- `silver.teams_info`

### Columns
| Column Name       | Data Type | Description                              |
|-------------------|-----------|------------------------------------------|
| `team_key`        | BIGINT    | Unique surrogate key for each team, generated using ROW_NUMBER() ordered by `team_code`. |
| `team_code`       | TEXT      | Primary team identifier from the source system. |
| `team_name`       | TEXT      | Full official name of the team.          |
| `team_short_name` | TEXT      | Abbreviated team name for compact displays/reports. |
| `team_logo`       | TEXT      | URL reference to the team’s logo image.  |

### Notes
- Results are ordered by `team_code ASC` for consistency.

---

## View: `gold.v_dimension_players`

### Description
A dimensional view providing standardized, unique-keyed reference data for players.

### Purpose
Serves as a reliable lookup table for player information with consistent surrogate keys.

### Source
- `silver.players_info`

### Columns
| Column Name    | Data Type | Description                              |
|----------------|-----------|------------------------------------------|
| `player_key`   | BIGINT    | Unique surrogate key for each player, generated using ROW_NUMBER() ordered by `player_code`. |
| `player_code`  | TEXT      | Primary player identifier from the source system. |
| `first_name`   | TEXT      | Player’s given first name.               |
| `second_name`  | TEXT      | Player’s surname or family name.         |
| `web_name`     | TEXT      | Player’s display name for web interfaces. |
| `player_photo` | TEXT      | URL reference to the player’s photo.     |

### Notes
- Results are ordered by `player_code ASC` for consistency.

---

## View: `gold.v_fct_player_history`

### Description
A fact table tracking historical player data with unique identifiers per record.

### Purpose
Provides historical facts for players, such as position and season, for analysis.

### Source
- `silver.players_info`

### Columns
| Column Name      | Data Type | Description                              |
|------------------|-----------|------------------------------------------|
| `history_id`     | BIGINT    | Unique surrogate key for each history record, generated using ROW_NUMBER() ordered by `player_code`. |
| `player_code`    | TEXT      | Primary player identifier from the source system. |
| `player_position`| TEXT      | Player’s position on the team (e.g., forward, defender). |
| `player_region`  | TEXT      | Geographic region associated with the player. |
| `season`         | INT       | Season identifier extracted from the last 8 characters of `dwh_team_id`. |

### Notes
- Ordered by `player_code` and `dwh_ingestion_date ASC`.

---

## View: `gold.v_fct_standing`

### Description
A fact table tracking team standings throughout the season, including game-week performance.

### Purpose
Supports team position trend analysis based on game results and cumulative points.

### Source
- `silver.teams_info`
- `silver.games_info` (via subquery)

### Columns
| Column Name    | Data Type | Description                              |
|----------------|-----------|------------------------------------------|
| `game_code`    | TEXT      | Unique identifier for each game.         |
| `game_week_id` | INT       | Week number within the season.           |
| `game_id`      | INT       | Internal game identifier.                |
| `team_code`    | TEXT      | Team’s unique code from `teams_info`.    |
| `goals_for`    | INT       | Goals scored by the team in this game.   |
| `goals_against`| INT       | Goals conceded by the team in this game. |
| `home_away`    | TEXT      | ‘H’ for home, ‘A’ for away.              |
| `game_status`  | TEXT      | ‘W’ for win, ‘L’ for loss, ‘D’ for draw. |
| `points`       | INT       | Points earned in this game (3 for win, 1 for draw, 0 for loss). |
| `cumm_points`  | INT       | Cumulative points up to this game week.  |
| `position`     | INT       | Team ranking within season and game week, based on points. |
| `season`       | TEXT      | Season identifier (e.g., year or name).  |

### Notes
- Ordered by `season`, `game_week_id`, and `game_code ASC`.

---

## View: `gold.v_fct_future_games`

### Description
Stores detailed game-by-game data for future games with home and away team information.

### Purpose
Provides a reference for upcoming game schedules and team matchups.

### Source
- `silver.games_info`
- `silver.teams_info`

### Columns
| Column Name     | Data Type | Description                              |
|-----------------|-----------|------------------------------------------|
| `game_code`     | TEXT      | Unique identifier for each game.         |
| `game_week_id`  | INT       | Week number within the season.           |
| `kickoff_time`  | TIMESTAMP | Scheduled start time of the game.        |
| `home_team_code`| TEXT      | Home team’s code from `teams_info`.      |
| `away_team_code`| TEXT      | Away team’s code (via subquery).         |
| `difficulty_h`  | INT       | Home team difficulty metric.             |
| `difficulty_a`  | INT       | Away team difficulty metric.             |
| `season`        | TEXT      | Season identifier (e.g., year or name).  |

### Notes
- Ordered by `game_code` and `game_week_id ASC`.

---

## View: `gold.v_fct_results`

### Description
Stores detailed game-by-game results with home and away team information.

### Purpose
Provides historical game outcomes for analysis.

### Source
- `silver.games_info`
- `silver.teams_info`

### Columns
| Column Name     | Data Type | Description                              |
|-----------------|-----------|------------------------------------------|
| `game_code`     | TEXT      | Unique identifier for each game.         |
| `game_week_id`  | INT       | Week number within the season.           |
| `kickoff_time`  | TIMESTAMP | Scheduled start time of the game.        |
| `home_team_code`| TEXT      | Home team’s code from `teams_info`.      |
| `game_score`    | TEXT      | Score in format “home-away” (e.g., “2-1”). |
| `away_team_code`| TEXT      | Away team’s code (via subquery).         |
| `difficulty_h`  | INT       | Home team difficulty metric.             |
| `difficulty_a`  | INT       | Away team difficulty metric.             |
| `season`        | TEXT      | Season identifier (e.g., year or name).  |

### Notes
- Ordered by `game_code` and `game_week_id ASC`.

---

## View: `gold.v_fct_player_stats`

### Description
Stores player statistics for each game week, linked to dimensional references.

### Purpose
Tracks detailed stats per player per game for performance analysis.

### Source
- `silver.player_stats`
- `silver.players_info`
- `gold.v_dimension_stat_type`

### Columns
| Column Name   | Data Type | Description                              |
|---------------|-----------|------------------------------------------|
| `game_code`   | TEXT      | Unique code identifying the game.        |
| `game_id`     | INT       | Numeric identifier for the game.         |
| `value`       | INT       | Recorded value of the statistic.         |
| `player_code` | TEXT      | Player’s unique code from `players_info`. |
| `season`      | TEXT      | Season during which stats were recorded. |
| `stats_key`   | BIGINT    | Foreign key linking to `v_dimension_stat_type`. |

### Notes
- Ordered by `game_code` and `player_code ASC`.

---

## View: `gold.v_dimension_season`

### Description
A dimensional view providing a lookup for all seasons in the database.

### Purpose
Serves as a reference table for season data.

### Source
- `silver.games_info`

### Columns
| Column Name   | Data Type | Description                              |
|---------------|-----------|------------------------------------------|
| `season_name` | TEXT      | Formatted season name (e.g., “2023/2024”). |
| `season`      | INT       | Raw season identifier (e.g., year).      |

### Notes
- Ordered by `season ASC`.

---

## View: `gold.v_dimension_stat_type`

### Description
A dimensional view cataloging available player statistic types.

### Purpose
Serves as a reference for stat types linked to player stats facts.

### Source
- `silver.player_stats`

### Columns
| Column Name | Data Type | Description                              |
|-------------|-----------|------------------------------------------|
| `stat_key`  | BIGINT    | Unique surrogate key for each stat type, generated using ROW_NUMBER(). |
| `stat_type` | TEXT      | Type of statistic (e.g., goals, assists). |

### Notes
- Ordered by `stat_type ASC`.

---