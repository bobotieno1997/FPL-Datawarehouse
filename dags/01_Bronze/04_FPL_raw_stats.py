import requests
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.getenv('location'))

# Define stats list globally
stats_list = ["goals_scored", "own_goals", "yellow_cards", "red_cards", "assists",
              "penalties_saved", "penalties_missed", "saves", "bonus", "bps"]

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def pull_data_from_api():
    """Fetch data from Fantasy Premier League API."""
    api_url = "https://fantasy.premierleague.com/api/fixtures/?future=0"
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        logging.info("✅ API data fetched successfully")
        return response.json()
    except requests.RequestException as e:
        logging.error(f"❌ Error fetching data from API: {e}")
        raise

def extract_stats(data, identifier):
    """Extract relevant match statistics."""
    if not data:
        raise ValueError("❌ API response is empty.")

    extracted_details = []
    for fixture in data:
        stats = fixture.get("stats", [])
        for stat in stats:
            if stat.get("identifier") == identifier:
                for team_type in ['a', 'h']:
                    for entry in stat.get(team_type, []):
                        extracted_details.append({
                            'game_code': fixture.get('code', None),
                            'finished': fixture.get('finished', None),
                            'game_id': fixture.get('id', None),
                            'stat_value': entry.get('value', None),
                            'player_id': entry.get('element', None),
                            'team_type': team_type
                        })
    return pd.DataFrame(extracted_details)

def join_dataframes(data):
    """Process extracted data into relevant statistics."""
    if data is None or len(data) == 0:
        raise ValueError("❌ No data available for processing.")

    df_list = []
    for stat in stats_list:
        df = extract_stats(data, stat)
        if not df.empty:
            df['stat_type'] = stat
            df_list.append(df)

    new_df = pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()
    logging.info("✅ Data processing completed successfully")
    return new_df

def upload_to_postgres(df):
    """Upload DataFrame to PostgreSQL."""
    # Retrieve the environment variables
    dbname = os.getenv('dbname')
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = os.getenv('port')

    print()

    # Validate database parameters
    if not all([dbname, user, password, host, port]):
        raise ValueError("❌ Missing database connection parameters")

    if df is None or df.empty:
        logging.warning("⚠️ DataFrame is empty, skipping upload")
        return  # Skip rather than raise error

    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
        logging.info("✅ Database connection established")
        
        df.to_sql(
            'player_stats',
            engine,
            schema='bronze',
            if_exists='replace',  # Changed to append instead of replace
            index=False,
            method='multi'  # Added for better performance with PostgreSQL
        )
        logging.info("✅ Data loaded into 'bronze.player_stats' successfully")
    except Exception as e:
        logging.error(f"❌ Failed to process database operation: {e}")
        raise
    finally:
        if 'engine' in locals():
            engine.dispose()  # Clean up connection

# Example of how to call the functions:
if __name__ == "__main__":
    data = pull_data_from_api()
    combined_stats = join_dataframes(data)
    upload_to_postgres(combined_stats)



    



