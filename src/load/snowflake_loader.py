import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import logging
import pandas as pd
from src.utils.constants import SF_DATABASE, SF_SCHEMA

load_dotenv()
logging.basicConfig(format='%(asctime)s: %(levelname)s - %(message)s')

def snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"))

def loading_data_into_snowflake(conn, df, table_name):
    # Ensuring all column names are in upper case
    df.columns = [col.upper() for col in df.columns]
    try:
        success, n_chunks, n_rows, _ = write_pandas(
            conn=conn,
            df=df,
            database=SF_DATABASE,
            schema=SF_SCHEMA,
            table_name=table_name,
            auto_create_table=False,
            overwrite=True
        )
        print(f"Successfully loaded {n_rows} rows into {table_name}.")
    except Exception as e:
        logging.error(f"Failed to load data: {e}")

