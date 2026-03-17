import os
import requests
import json
from dotenv import load_dotenv
import logging
import time

logging.basicConfig(format='%(asctime)s: %(levelname)s - %(message)s', level=logging.INFO)
load_dotenv()


class TMDBClient:
    def __init__(self):
        self.api_key = os.getenv("API_KEY")
        self.base_url = "https://api.themoviedb.org/3"

    def get_enriched_movies(self, pages=5):
        enriched_data = []
        movie_ids = []

        # Getting and saving all the movie_ids
        for page in range(1, pages + 1):
            url = f"{self.base_url}/movie/popular"
            params = {"api_key": self.api_key, "page": page}
            response = requests.get(url, params=params)

            if response.status_code == 200:
                logging.info(f"Collecting movies ids from page {page}")
                movie_ids.extend([m['id'] for m in response.json()['results']])
            else:
                logging.error(f"Failed to get movie ids: status_code {response.status_code}")

        logging.info(f"Found {len(movie_ids)} movies, begin collecting details and credits of each movie")
        for movie_id in movie_ids:
            # append_to_response combines details and credits into one JSON
            detail_url = f"{self.base_url}/movie/{movie_id}"
            params = {"api_key": self.api_key, "append_to_response": "credits"}
            response = requests.get(detail_url, params=params)

            if response.status_code == 200:
                # logging.info(f"Collecting movies details and credits ")
                enriched_data.append(response.json())
            else:
                logging.error(f"Error enrich movie: {movie_id}")

            # TMDB limit is about 50 req/sec, so this is a safeguard
            # time.sleep(0.05)

        return enriched_data

    # def get_genres(self):
    #     url = f"{self.base_url}/genre/movie/list"
    #     params = {"api_key": self.api_key}
    #     response = requests.get(url, params=params)
    #     return response.json()

    def save_to_json(self, json_data, filepath) -> None:

        with open(filepath, "w") as file:
            logging.info(f"Saving json data to {filepath}")
            json.dump(json_data, file, indent=4)

