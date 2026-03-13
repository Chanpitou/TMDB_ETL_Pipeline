import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import logging
from src.utils.constants import SF_DATABASE, SF_SCHEMA

load_dotenv()
logging.basicConfig(format='%(asctime)s: %(levelname)s - %(message)s')

def snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv["user"],
        password=os.getenv["password"],
        account=os.getenv["account"],
        warehouse=os.getenv["warehouse"],
        database=os.getenv["database"],
        schema=os.getenv["schema"],
        role=os.getenv["role"])

def loading_data_into_snowflake(df, table_name):
    connection = snowflake_connection()
    try:
        success, n_chunks, n_rows, _ = write_pandas(
            conn=connection,
            df=df,
            database=SF_DATABASE,
            schema=SF_SCHEMA,
            table_name=table_name,
            auto_create_table=True,
            overwrite=True
        )
        print(f"Successfully loaded {n_rows} rows.")
    except Exception as e:
        logging.error(f"Failed to load data: {e}")