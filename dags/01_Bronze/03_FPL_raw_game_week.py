import requests
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('/home/luxds/Bob/Fantasy/.env')

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def pull_data_from_api():
    """Fetch data from Fantasy Premier League API."""
    api_url = "https://fantasy.premierleague.com/api/fixtures/"
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        logging.info("✅ API data fetched successfully")
        return response.json()
    except requests.RequestException as e:
        logging.error(f"❌ Error fetching data from API: {e}")
        raise

def create_data_frame(data):
    """Convert API response to a Pandas DataFrame."""
    if not data:
        raise ValueError("❌ API response is empty.")

    try:
        df = pd.DataFrame(data)

        # Select relevant columns
        selected_columns = [
            'code', 'event', 'finished', 'id', 'kickoff_time', 'team_a', 'team_h', 
            'team_a_score', 'team_h_score', 'team_a_difficulty', 'team_h_difficulty'
        ]
        df = df[selected_columns]

        # Rename columns for clarity
        column_mapping = {
            "code": "game_code",
            "event": "game_week_id",
            "id": "game_id",
            "team_a": "team_id_a",
            "team_h": "team_id_h",
            "team_a_difficulty": "difficulty_a",
            "team_h_difficulty": "difficulty_h",
        }
        df.rename(columns=column_mapping, inplace=True)

        # Convert data types
        df["kickoff_time"] = pd.to_datetime(df["kickoff_time"])
        df.fillna(0, inplace=True)  # Replace NaN values with zero where applicable

        logging.info(f"✅ DataFrame created with {len(df)} records.")
        return df
    except Exception as e:
        logging.error(f"❌ Error processing API response: {e}")
        raise

def upload_to_postgres(df):
    """Upload DataFrame to PostgreSQL."""
    # Retrieve the environment variables
    dbname = os.getenv('dbname')
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = os.getenv('port')

    # Check if DataFrame is empty
    if df is None or df.empty:
        raise ValueError("❌ DataFrame is empty, cannot upload to PostgreSQL.")

    # Create SQLAlchemy engine for PostgreSQL connection   
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
        logging.info("✅ Database connection established")
    except Exception as e:
        logging.error(f"❌ Failed to connect to database: {e}")
        raise

    # Load DataFrame into the 'bronze' schema
    try:
        df.to_sql(
            'games_info',           # Table name
            engine,                 # SQLAlchemy engine
            schema='bronze',        # Target schema
            if_exists='replace',    # 'replace' to overwrite, 'append' to add data
            index=False             # Exclude DataFrame index
        )
        logging.info("✅ Data loaded into 'bronze.games_info' successfully")
    except Exception as e:
        logging.error(f"❌ Failed to load data into database: {e}")
        raise

# Example of how to call the functions:
if __name__ == "__main__":
    data = pull_data_from_api()
    df = create_data_frame(data)
    upload_to_postgres(df)

