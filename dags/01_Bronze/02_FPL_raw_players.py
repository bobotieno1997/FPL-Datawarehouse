import requests
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

#Load Environment Variables
load_dotenv(os.getenv('location'))

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def pull_data_from_api():
    """Fetch data from Fantasy Premier League API."""
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses
        data = response.json()
        logging.info("✅ Players data fetched successfully")
        # Get date for season categorization
        dates = []
        for event in data['events']:
            _date = event.get('deadline_time')  # Fixed: Use event, not stats
            if _date:
                dates.append(_date)

        if not dates:
            raise ValueError("❌ No deadline_time found in events.")
        
        max_date = max(dates)
        min_date = min(dates)
        return {"data": data, "min_date": min_date, "max_date": max_date}

    except requests.RequestException as e:
        logging.error(f"❌ Error fetching data from API: {e}")
        raise

def create_data_frame(raw_data):
    """Convert API response to a Pandas DataFrame."""
    data = raw_data["data"]
    min_date = raw_data["min_date"]
    max_date = raw_data["max_date"]

    if not data or "elements" not in data:
        raise ValueError("❌ API response is empty or malformed.")

    try:
        df = pd.DataFrame(data["elements"], columns=[
            "id", "first_name", "second_name", "web_name", "team_code",
            "team", "element_type", "code", "region", "can_select"
        ])

        # Rename columns for clarity
        df.rename(columns={
            "id": "player_id",
            "element_type": "player_position",
            "team": "team_id",
            "code": "player_code"
        }, inplace=True)
        df['min_kickoff'] = min_date
        df['max_kickoff'] = max_date

        logging.info(f"✅ DataFrame created with {len(df)} records.")
        return df
    except Exception as e:
        logging.error(f"❌ Error converting data to DataFrame: {e}")
        raise

def add_player_image(df):
    """Add player image URLs to DataFrame."""
    if df is None or df.empty:
        raise ValueError("❌ DataFrame is empty, cannot add player images.")

    try:
        base_url = "https://resources.premierleague.com/premierleague/photos/players/250x250/p"
        df["photo_url"] = df["player_code"].astype(str).apply(lambda code: f"{base_url}{code}.png")
        logging.info("✅ Player image URLs added successfully.")
        return df
    except Exception as e:
        logging.error(f"❌ Error adding player images: {e}")
        raise

def upload_to_postgres(df):
    """Upload DataFrame to PostgreSQL bronze.teams_info table."""
   
    # Retrieve the environment variables
    dbname = os.getenv('dbname')
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = os.getenv('port')

    # Evaluate if df contains data
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
            'players_info',           # Table name
            engine,                   # SQLAlchemy engine
            schema='bronze',          # Target schema
            if_exists='replace',      # 'replace' to overwrite, 'append' to add data
            index=False               # Exclude DataFrame index
        )
        logging.info("✅ Data loaded into 'bronze.players_info' successfully")
    except Exception as e:
        logging.error(f"❌ Failed to load data into database: {e}")
        raise

# Main script to execute the functions
def main():
    raw_data = pull_data_from_api()
    df = create_data_frame(raw_data)
    enriched = add_player_image(df)
    upload_to_postgres(enriched)

if __name__ == "__main__":
    main()


