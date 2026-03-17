import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

from babel.util import PYTHON_FUTURE_IMPORT_re

# Allowing Airflow to find 'src' folder
sys.path.append('/opt/airflow')

from src.extract.extract_api_data import TMDBClient
from src.transform.transformations import (fetch_json_data, transform_dim_movies,
                                           transform_fact_finance, transform_cast_data,
                                           transform_crew_data, transform_genre_data)
from src.load.snowflake_loader import snowflake_connection, loading_data_into_snowflake
from src.utils.db_utils import load_json_data

logging.basicConfig(format='%(asctime)s: %(levelname)s - %(message)s', level=logging.INFO)

default_args = {
    'owner': 'chanpitou',
    'start_date': datetime(2026, 3, 15),
    # 'retries': 2,
    # 'retry_delay': timedelta(minutes=5),
}


def run_extraction():
    logging.info("Starting TMDB ETL Pipeline")
    logging.info("------------------ Begin data extraction ------------------")
    client = TMDBClient()

    enriched_movies = client.get_enriched_movies(pages=10)
    client.save_to_json(enriched_movies, "/opt/airflow/data/enriched_movies.json")

    logging.info("Loading raw movie data to postgres staging database")
    load_json_data(enriched_movies)
    logging.info("------------------ End data extraction ------------------")

def run_transformation_load_to_snowflake():
    logging.info("------------------ Begin data transformation ------------------")
    logging.info("Fetching raw movie data from staging table")
    fetched_raw_df = fetch_json_data()

    logging.info("DIM_MOVIES transformation")
    dim_movies = transform_dim_movies(fetched_raw_df)

    logging.info("FACT_FINANCE transformation")
    fact_movie_finance = transform_fact_finance(fetched_raw_df)

    logging.info("DIM_CAST_PEOPLE & FACT_CAST transformation")
    dim_cast_people, fact_cast = transform_cast_data(fetched_raw_df)

    logging.info("DIM_CREW_PEOPLE & FACT_CREW transformation")
    dim_crew_people, fact_crew = transform_crew_data(fetched_raw_df)

    logging.info("DIM_GENRE & FACT_MOVIE_GENRE transformation")
    dim_genres = transform_genre_data(fetched_raw_df)

    logging.info("Combining DIM_CAST_PEOPLE & DIM_CREW_PEOPLE into DIM_PEOPLE")
    dim_people = pd.concat([dim_cast_people, dim_crew_people], ignore_index=True).drop_duplicates(subset=['id']).copy()
    logging.info("------------------ End data transformation ------------------")

    logging.info("------------------ Begin loading data to Snowflake ------------------")
    logging.info("Loading all dataframes into Snowflake")
    # Assigning a dict for table names and their df
    load_queue = {
        "DIM_MOVIES": dim_movies,
        "DIM_PEOPLE": dim_people,
        "DIM_GENRES": dim_genres,
        "FACT_MOVIE_FINANCE": fact_movie_finance,
        "FACT_CAST": fact_cast,
        "FACT_CREW": fact_crew
    }
    con = None
    try:
        con = snowflake_connection()
        for table_name, df in load_queue.items():
            logging.info(f"Loading data into {table_name}")
            loading_data_into_snowflake(con, df, table_name)
    except Exception as e:
        logging.error(f"Pipeline failed during load: {e}")
    finally:
        if con:
            logging.info("Closing snowflake connection.")
            con.close()
    logging.info("------------------ End data loading to Snowflake ------------------")

with DAG(
        'tmdb_snowflake_pipeline',
        default_args=default_args,
) as dag:
    extract = PythonOperator(
        task_id='extract_from_tmdb',
        python_callable=run_extraction
    )

    transform_load = PythonOperator(
        task_id='transform_load_to_snowflake',
        python_callable=run_transformation_load_to_snowflake
    )

    extract >> transform_load