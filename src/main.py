from src.extract.extract_api_data import TMDBClient
from src.utils.db_utils import load_json_data
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