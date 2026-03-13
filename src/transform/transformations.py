from src.utils.db_utils import connect_to_db
from src.utils.constants import DB_SCHEMA_RAW, DB_TABLE_RAW
import pandas as pd

def fetch_json_data():
    engine = connect_to_db()
    print(engine)
    query = f"SELECT data FROM {DB_SCHEMA_RAW}.{DB_TABLE_RAW}"

    df_raw = pd.read_sql_query(query, engine)
    return df_raw

def transform_dim_movies(df):
    movie_data = pd.json_normalize(df['data'], sep="_")
    # DIM_MOVIES: movies data
    dim_movies = movie_data[[
        'id', 'title', 'release_date', 'runtime',
        'original_language', 'status'
    ]].copy()

    dim_movies.rename(columns={'id': 'tmdb_id'}, inplace=True)
    return dim_movies


def transform_fact_finance(df):
    movie_data = pd.json_normalize(df['data'], sep="_")

    # FACT_FINANCE: movie performance
    fact_finance = movie_data[[
        'id', 'budget', 'revenue', 'popularity',
        'vote_average', 'vote_count'
    ]].copy()

    # Calculated net profit
    fact_finance['profit'] = fact_finance['revenue'] - fact_finance['budget']

    # Calculated Return on Investment
    fact_finance['roi'] = fact_finance.apply(
        lambda x: (x['profit'] / x['budget']) if x['budget'] > 0 else 0, axis=1
    )

    fact_finance.rename(columns={'id': 'tmdb_id'}, inplace=True)
    return fact_finance

def transform_cast_data(df):
    cast_raw = pd.json_normalize(df["data"],
                                 record_path=["credits", "cast"],
                                 record_prefix="cast_",
                                 meta="id",
                                 meta_prefix="movie_")

    # DIM_PEOPLE: unique cast from all the movies
    dim_cast_people = cast_raw[['cast_id', 'cast_name', 'cast_gender', 'cast_popularity']].drop_duplicates(subset=['cast_id']).copy()

    # FACT_CAST: The relationship between movie and actor
    fact_cast = cast_raw[['movie_id', 'cast_id', 'cast_character', 'cast_order']].copy()

    return dim_cast_people, fact_cast

def transform_crew_data(df):
    crew_raw = pd.json_normalize(df["data"],
                                 record_path=["credits", "crew"],
                                 record_prefix="crew_",
                                 meta="id",
                                 meta_prefix="movie_")
    # DIM_CREW_PEOPLE: unique crew from all movies
    dim_crew_people = crew_raw[['crew_id', 'crew_name', 'crew_gender', 'crew_popularity']].drop_duplicates(subset=['crew_id'])

    # FACT_CREW: The relationship between movie and crew
    fact_crew = crew_raw[["movie_id", "crew_id", "crew_job", "crew_department"]].copy()
    return dim_crew_people, fact_crew

def transform_genre_data(df):
    genre_raw = pd.json_normalize(df["data"],
                                  record_path="genres",
                                  record_prefix="genre_",
                                  meta="id",
                                  meta_prefix="movie_")
    # DIM_GENRES: Unique genre names
    dim_genre = genre_raw[['genre_id', 'genre_name']].drop_duplicates(subset=['genre_id']).copy()

    # BRIDGE: Link movies to genres
    fact_movie_genres = genre_raw[['movie_id', 'genre_id']].copy()
    return dim_genre, fact_movie_genres

if __name__ == "__main__":
    raw_df = fetch_json_data()
    dim_cast_people, fact_cast = transform_cast_data(raw_df)
    dim_crew_people, fact_crew = transform_crew_data(raw_df)
    dim_genre, fact_movie_genres = transform_genre_data(raw_df)

    print(dim_genre)
