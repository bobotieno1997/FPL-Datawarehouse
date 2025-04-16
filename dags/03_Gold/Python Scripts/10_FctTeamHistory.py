import psycopg2
from sqlalchemy import create_engine, text
import pandas as pd
import pymysql
import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.getenv('location'))

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_data_from_postgres(query: str) -> pd.DataFrame:
    """Pull data from PostgreSQL view and return a DataFrame"""
    try:
        # Postgres connection details
        host = os.getenv('host')
        port = os.getenv('port')
        dbname = os.getenv('dbname')
        user = os.getenv('user')
        password = os.getenv('password')

        if not all([host, port, dbname, user, password]):
            raise ValueError("❌ Missing PostgreSQL connection parameters")

        conn_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
        engine = create_engine(conn_str)
        df = pd.read_sql_query(query, engine)
        engine.dispose()

        logging.info(f"✅ Data fetched from PostgreSQL: {len(df)} rows")
        return df

    except Exception as e:
        logging.error(f"❌ Failed to fetch data from PostgreSQL: {e}")
        raise

def load_data_to_mysql(df: pd.DataFrame, table_name: str):
    """Truncate MySQL table and load DataFrame into it"""
    try:
        # MySQL connection details
        ghost = os.getenv('ghost')
        gport = os.getenv('gport')
        gdbname = os.getenv('gdbname')
        guser = os.getenv('guser')
        gpassword = os.getenv('gpassword')

        if not all([ghost, gport, gdbname, guser, gpassword]):
            raise ValueError("❌ Missing MySQL connection parameters")

        if df is None or df.empty:
            raise ValueError("❌ DataFrame is empty. Nothing to load into MySQL.")

        mysql_engine = create_engine(
            f'mysql+pymysql://{guser}:{gpassword}@{ghost}:{gport}/{gdbname}'
        )

        # Truncate the table before loading
        with mysql_engine.connect() as connection:
            connection.execute(text(f"TRUNCATE TABLE {table_name}"))
            logging.info(f"🧹 Table `{table_name}` truncated successfully")

        # Load data using append (preserves schema)
        df.to_sql(table_name, con=mysql_engine, if_exists='append', index=False)
        mysql_engine.dispose()

        logging.info(f"✅ Data loaded into MySQL table `{table_name}` successfully")

    except Exception as e:
        logging.error(f"❌ Failed to load data to MySQL: {e}")
        raise



def main():
    query = 'SELECT * FROM gold.v_fct_team_history'
    table_name = 'FctTeamHistory'

    df = fetch_data_from_postgres(query)
    df.head(5)
    load_data_to_mysql(df, table_name)

if __name__ == "__main__":
    main()


