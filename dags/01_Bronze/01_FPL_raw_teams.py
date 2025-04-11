import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load Environment Variables
load_dotenv(os.getenv('location'))

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def pull_data_from_api():
    """Fetch data from Fantasy Premier League API."""
    api_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        logging.info("✅ Teams data fetched successfully")

        # Get date for season categorization
        dates = []
        for event in data['events']:
            _date = event.get('deadline_time') 
            if _date:
                dates.append(_date)

        if not dates:
            raise ValueError("❌ No deadline_time found in events.")
        
        max_date = max(dates)
        min_date = min(dates)
        return {"data": data, "min_date": min_date,"max_date":max_date}  # Return as dict for clarity
    except requests.RequestException as e:
        logging.error(f"❌ Error fetching data from API: {e}")
        raise

def create_data_frame(raw_data):
    """Convert API response to a Pandas DataFrame."""
    data = raw_data["data"]
    min_date = raw_data["min_date"]
    max_date = raw_data["max_date"]

    if not data or "teams" not in data:
        raise ValueError("❌ API response is empty or malformed.")

    try:
        df = pd.DataFrame(data["teams"], columns=["id", "code", "name", "short_name"])
        df.rename(
            columns={
                "id": "team_id",
                "code": "team_code",
                "name": "team_name",
                "short_name": "team_short_name"
            }, 
            inplace=True
        )
        df['min_kickoff'] = min_date
        df['max_kickoff'] = max_date
        logging.info(f"✅ DataFrame created with {len(df)} records.")
        return df
    except Exception as e:
        logging.error(f"❌ Error converting data to DataFrame: {e}")
        raise

def add_photo_url(df):
    base_url = "https://resources.premierleague.com/premierleague/badges/t"
    # Fetch logo files from the GitHub repository
    df['logo_url'] = df['team_code'].apply(lambda code : f"{base_url}{code}.png")

    df.loc[df['team_name'] == 'Liverpool', 'logo_url'] = 'https://upload.wikimedia.org/wikipedia/en/thumb/0/0c/Liverpool_FC.svg/180px-Liverpool_FC.svg.png' 
    return df

def upload_to_postgres(df):
    """Upload DataFrame to PostgreSQL bronze.teams_info table."""
    
    # Retrieve the environment variables
    dbname = os.getenv('dbname')
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = os.getenv('port')

    # Check if environment variables are loaded
    if not all([dbname, user, password, host, port]):
        raise ValueError("❌ Missing database connection parameters")

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
            'teams_info',           
            engine,                 
            schema='bronze',        
            if_exists='replace',    
            index=False             
        )
        logging.info("✅ Data loaded into 'bronze.teams_info' successfully")
    except Exception as e:
        logging.error(f"❌ Failed to load data into database: {e}")
        raise

# Main script to execute the functions
def main():
    raw_data = pull_data_from_api()
    df = create_data_frame(raw_data)
    fdf = add_photo_url(df)
    upload_to_postgres(fdf)

if __name__ == "__main__":
    main()

