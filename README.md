# Fantasy Premier League

![FPL_logo](https://github.com/bobotieno1997/FPL/blob/9b4eddd462aee2402433df7c01296e20d24cbda3/Others/FPL-Statement-Lead.webp)

This repository contains SQL and Python scripts for managing the Fantasy Premier League (FPL) dataset. Data is accessed via RESTful API endpoints provided by FPL and ingested using Python scripts. During ingestion, minimal transformations are applied before loading the data into a PostgreSQL instance hosted on Aiven.

---
## Architecture Overview
The Medallion architecture has been adopted as the solution approach, as FPL data is only available at the season level. For example, if the current season is 2024/2025, only data for that season is accessible.

![Architecture](https://github.com/bobotieno1997/FPL-Datawarehouse/blob/7ce01786f2f14156adaf3d5e4c338c796be1fa1e/project_files/Architecture/overview_architecture%20.jpg)

### 🥉 Bronze Layer

The **Bronze Layer** acts as the initial landing zone for all raw incoming datasets. It ensures data availability and reliability before further transformation in the Silver Layer. This layer is **fully refreshed** with each pipeline run, making it the foundation for the entire data processing workflow.

You can view the ELT scripts responsible for loading data into the bronze database [here](https://github.com/bobotieno1997/FPL-Datawarehouse/tree/main/dags/01_Bronze).

The code is orchestrated by the following Airflow DAG:

```python
import psycopg2
from airflow import DAG
from datetime import datetime, timedelta
import logging
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Activate virtual environment
virtual_env = 'source /home/luxds/Bob/Fantasy/venv/bin/activate'

# DAG default arguments
default_args = {
    "owner": "Bob Otieno",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 7),
    "email": ["bobotieno99@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Define Airflow DAG
with DAG(
    "Fantasy_PL_Datawarehouse", 
    default_args=default_args,
    schedule="@daily",
    catchup=False, 
    tags=["Bob Otieno", "FPL pipeline"],
) as dag:
    
    # Bronze Layer - Raw data ingestion scripts
    Run_teams_data = BashOperator(
        task_id='Load_Teams_Info',
        bash_command=f'{virtual_env} && python3 /home/luxds/Bob/Fantasy/dags/01_Bronze/01_FPL_raw_teams.py'
    )

    Run_teams_players = BashOperator(
        task_id='Load_Players_Info',
        bash_command=f'{virtual_env} && python3 /home/luxds/Bob/Fantasy/dags/01_Bronze/02_FPL_raw_players.py'
    )

    Run_gameweek_info = BashOperator(
        task_id='Load_gameweek_Info',
        bash_command=f'{virtual_env} && python3 /home/luxds/Bob/Fantasy/dags/01_Bronze/03_FPL_raw_game_week.py'
    )

    Run_players_stats = BashOperator(
        task_id='Load_Player_stats',
        bash_command=f'{virtual_env} && python3 /home/luxds/Bob/Fantasy/dags/01_Bronze/04_FPL_raw_stats.py'
    )

    # Silver Layer - Data transformations & procedures
    Run_usp_teams_info = SQLExecuteQueryOperator(
        task_id="Load_teams_info_silver",
        conn_id="FPL_db",  
        sql="CALL silver.usp_update_team_info();"
    )

    Run_usp_player_info = SQLExecuteQueryOperator(
        task_id="Load_players_info_silver",
        conn_id="FPL_db",  
        sql="CALL silver.usp_update_player_info();"
    )

    Run_usp_games_info = SQLExecuteQueryOperator(
        task_id="Load_players_games_silver",
        conn_id="FPL_db",  
        sql="CALL silver.usp_update_games_info();"
    )

    Run_usp_future_games_info = SQLExecuteQueryOperator(
        task_id="Load_future_games_silver",
        conn_id="FPL_db",  
        sql="CALL silver.usp_update_future_games_info();"
    )

    Run_usp_player_stats = SQLExecuteQueryOperator(
        task_id="Load_player_stats_silver",
        conn_id="FPL_db",  
        sql="CALL silver.usp_update_players_stats();"
    )

    # Task dependencies
    [Run_teams_data, Run_teams_players, Run_gameweek_info, Run_players_stats] >> \
    Run_usp_teams_info >> Run_usp_player_info >> Run_usp_games_info >> \
    Run_usp_future_games_info >> Run_usp_player_stats
```
Here’s the visual representation of the DAG execution flow:
![Airflow](https://github.com/bobotieno1997/FPL-Datawarehouse/blob/3c80cc938e837b3cdafc123f8afb7909bcb5ca6e/project_files/Other%20files/airflow_dags.png)

Sample data from the tables:

![Database Query](https://github.com/bobotieno1997/FPL-Datawarehouse/blob/7ce01786f2f14156adaf3d5e4c338c796be1fa1e/project_files/Other%20files/bronze_table.png)

### Silver Layer
The silver layer is the core data warehouse, designed to store historical data, including previous seasons. Unlike the bronze layer, which is refreshed every run, the silver layer is updated incrementally—only new or modified records are added to preserve history. The only exception is `silver.future_games_info`, which is truncated before each run.

Indexing is applied to frequently queried tables to enhance performance.

Click [here](https://github.com/bobotieno1997/FPL-Datawarehouse/tree/main/dags/02_Silver/Scripts) to view the stored procedure scripts used for updating the silver layer tables.

Sample data from the tables:

![Database Query](https://github.com/bobotieno1997/FPL-Datawarehouse/blob/7ce01786f2f14156adaf3d5e4c338c796be1fa1e/project_files/Other%20files/silver_table.png)

### Gold Layer
The gold layer is the reporting layer, where views are created from the silver layer to answer business questions. Virtual tables are used to optimize database storage and efficiency.

This layer follows a star schema for efficient querying. The processed data is optimized for analytical workloads and data visualization, enabling insightful reporting for FPL enthusiasts and analysts.

Click [here](https://github.com/bobotieno1997/FPL-Datawarehouse/tree/main/dags/03_Gold) to view the view scripts used for updating the gold layer tables.

Sample data from the tables:

![Database Query](https://github.com/bobotieno1997/FPL-Datawarehouse/blob/7ce01786f2f14156adaf3d5e4c338c796be1fa1e/project_files/Other%20files/gold_table.png)

## Technologies Used
- PostgreSQL Database
- Python Programming 
- Apache Airflow

## 📂 Repository Structure (Key Documents)
```
├───dags
│   ├───00_Initialization -- Database and schema creation scripts
│   ├───01_Bronze
│   │   ├───Scripts       -- Scripts to load data into the bronze layer
│   ├───02_Silver
│   │   ├───Scripts
│   │   │   ├───01_teams    -- Stored procedures for updating the teams silver layer table
│   │   │   ├───02_players  -- Stored procedures for updating the players silver layer table
│   │   │   ├───03_games    -- Stored procedures for updating the games silver layer table
│   │   │   └───04_stats    -- Stored procedures for updating the stats silver layer table
│   │   └───__pycache__
│   └───03_Gold            -- Scripts to create gold layer views
├───logs
├───plugins
└───project_files
    ├───Architecture      -- Architecture images and data flow
    └───Documentation     -- Naming conventions
``