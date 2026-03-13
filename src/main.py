from src.extract.extract_api_data import TMDBClient
from src.utils.db_utils import load_json_data
from src.transform.transformations import (fetch_json_data, transform_dim_movies,
                                           transform_fact_finance, transform_cast_data,
                                           transform_crew_data, transform_genre_data)
import logging

logging.basicConfig(format='%(asctime)s: %(levelname)s - %(message)s', level=logging.INFO)

logging.info("Executing Program")
logging.info("------------------ Begin data extraction ------------------")
client = TMDBClient()
enriched_movies = client.get_enriched_movies(pages=10)
# movie_genres = client.get_genres()

client.save_to_json(enriched_movies, "enriched_movies.json")
# client.save_to_json(movie_genres, "movie_genres.json")

logging.info("Loading raw movie data to postgres stage")
load_json_data(enriched_movies)
logging.info("------------------ End data extraction ------------------")
logging.info("------------------ Begin data transformation ------------------")
logging.info("Fetching raw movie data from staging table")
fetched_raw_df = fetch_json_data()

logging.info("DIM_MOVIES transformation")
dim_movies = transform_dim_movies(fetched_raw_df)

logging.info("FACT_FINANCE transformation")
fact_finance = transform_fact_finance(dim_movies)

logging.info("DIM_CAST_PEOPLE & FACT_CAST transformation")
dim_cast_people, fact_cast = transform_cast_data(fetched_raw_df)

logging.info("DIM_CREW_PEOPLE & FACT_CREW transformation")
dim_crew_people, fact_crew = transform_crew_data(fetched_raw_df)

logging.info("DIM_GENRE & FACT_MOVIE_GENRE transformation")
dim_genre, fact_movie_genres = transform_genre_data(fetched_raw_df)
logging.info("------------------ End data transformation ------------------")
