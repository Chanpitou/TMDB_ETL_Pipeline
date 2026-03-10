import os
import sqlalchemy
from sqlalchemy import text

from src.utils.constants import DB_SCHEMA_RAW ,DB_TABLE_RAW
import json
import logging

logging.basicConfig(format='%(asctime)s: %(levelname)s - %(message)s', level=logging.INFO)

db_schema = DB_SCHEMA_RAW
db_table = DB_TABLE_RAW

def connect_to_db():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")

    try:
        connection_uri = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        db_engine = sqlalchemy.create_engine(connection_uri)
        logging.info("Connection to PostgreSQL successful")
        return db_engine
    except Exception as e:
        logging.error(f"Failed to connect to PostgreSQL: {e}")


def load_json_data(json_data):
    engine = connect_to_db()

    upsert_query = text(f"""
            INSERT INTO {DB_SCHEMA_RAW}.{DB_TABLE_RAW} (tmdb_id, data)
            VALUES (:tmdb_id, :data)
            ON CONFLICT (tmdb_id) 
            DO UPDATE SET data = EXCLUDED.data, extracted_at = CURRENT_TIMESTAMP;;
        """)

    with engine.connect() as conn:
        params = [
            {"tmdb_id": movie.get("id"), "data": json.dumps(movie)}
            for movie in json_data
        ]
        logging.info("Executing upsert query")
        print(upsert_query)
        conn.execute(upsert_query, params)
        conn.commit()
        logging.info("Upsert query successful")


