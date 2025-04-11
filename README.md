# Fantasy Premier League

![FPL_logo](https://github.com/bobotieno1997/FPL/blob/9b4eddd462aee2402433df7c01296e20d24cbda3/Others/FPL-Statement-Lead.webp)

This repository contains SQL and Python scripts for managing the Fantasy Premier League (FPL) dataset. Data is accessed via RESTful API endpoints provided by FPL and ingested using Python scripts. During ingestion, minimal transformations are applied before loading the data into a PostgreSQL instance hosted on Aiven.

---
## Architecture Overview
The Medallion architecture has been adopted as the solution approach, as FPL data is only available at the season level. For example, if the current season is 2024/2025, only data for that season is accessible.

![Architecture](https://github.com/bobotieno1997/Fantsy-Premier-League-DWH/blob/5106904ca942be83605a357b4138906b229918ba/project_files/Architecture/overview_architecture%20.jpg)

### Bronze Layer
The bronze layer serves as the landing zone for all incoming datasets, ensuring data availability for processing before being loaded into the silver layer. This layer is fully refreshed during every run.

Click [here](https://github.com/bobotieno1997/Fantsy-Premier-League-DWH/tree/main/dags/01_Bronze) to view the Airflow DAGs responsible for loading data into the bronze database.

Example of Airflow web server UI displaying successful DAG runs and tables:

![Airflow](https://github.com/bobotieno1997/Fantsy-Premier-League-DWH/blob/dfe69cdfe176465d558871c110e445cc12290dc8/project_files/Other%20files/bronze_airflow.png)

Airflow logs are stored in s3 bucket for scalability reasons
![s3_logs](https://github.com/bobotieno1997/Fantsy-Premier-League-DWH/blob/dfe69cdfe176465d558871c110e445cc12290dc8/project_files/Other%20files/bronze_airflow.png)

Sample data from the tables:

![Database Query](https://github.com/bobotieno1997/Fantsy-Premier-League-DWH/blob/dfe69cdfe176465d558871c110e445cc12290dc8/project_files/Other%20files/bronze_table.png)

### Silver Layer
The silver layer is the core data warehouse, designed to store historical data, including previous seasons. Unlike the bronze layer, which is refreshed every run, the silver layer is updated incrementally—only new or modified records are added to preserve history. The only exception is `silver.future_games_info`, which is truncated before each run.

Indexing is applied to frequently queried tables to enhance performance.

Click [here](https://github.com/bobotieno1997/Fantsy-Premier-League-DWH/tree/main/dags/02_Silver/Scripts) to view the stored procedure scripts used for updating the silver layer tables.

Sample data from the tables:

![Database Query](https://github.com/bobotieno1997/Fantsy-Premier-League-DWH/blob/dfe69cdfe176465d558871c110e445cc12290dc8/project_files/Other%20files/silver_table.png)

### Gold Layer
The gold layer is the reporting layer, where views are created from the silver layer to answer business questions. Virtual tables are used to optimize database storage and efficiency.

This layer follows a star schema for efficient querying. The processed data is optimized for analytical workloads and data visualization, enabling insightful reporting for FPL enthusiasts and analysts.

Click [here](https://github.com/bobotieno1997/Fantsy-Premier-League-DWH/tree/main/dags/03_Gold) to view the view scripts used for updating the gold layer tables.

Sample data from the tables:

![Database Query](https://github.com/bobotieno1997/Fantsy-Premier-League-DWH/blob/dfe69cdfe176465d558871c110e445cc12290dc8/project_files/Other%20files/gold_table.png)

## Technologies Used
- PostgreSQL Database
- Python Programming 
- Docker
- Apache Airflow

## 📂 Repository Structure (Key Documents)
```
├───config
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
```
