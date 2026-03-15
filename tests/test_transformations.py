import pytest
from src.transform.transformations import *

@pytest.fixture
def raw_data():
    file_path = "data/enriched_movies.json"
    raw_json_data = pd.read_json(file_path)
    return pd.DataFrame({"data": raw_json_data.to_dict(orient='records')})

def test_transform_dim_movies(raw_data):
    """ Test transformation, Ensuring primary key exists and not null """
    dim_movies_df = transform_dim_movies(raw_data)

    assert "tmdb_id" in dim_movies_df.columns
    assert dim_movies_df["tmdb_id"].isnull().sum() == 0

def test_transform_fact_finance(raw_data):
    """ Test transformation, Ensuring primary key exists and not null """
    fact_finance_df = transform_fact_finance(raw_data)

    assert "tmdb_id" in fact_finance_df.columns
    assert fact_finance_df["tmdb_id"].isnull().sum() == 0

def test_transform_cast_data(raw_data):
    """ Test transformation, Ensuring primary key exists and not null """
    dim_cast_people_df, fact_cast_df = transform_cast_data(raw_data)

    assert "id" in dim_cast_people_df.columns
    assert dim_cast_people_df["id"].isnull().sum() == 0

    assert "person_id" in fact_cast_df.columns
    assert fact_cast_df["person_id"].isnull().sum() == 0


def test_transform_crew_data(raw_data):
    """ Test transformation, Ensuring primary key exists and not null """
    dim_crew_people_df, fact_crew_df = transform_crew_data(raw_data)

    assert "id" in dim_crew_people_df.columns
    assert dim_crew_people_df["id"].isnull().sum() == 0

    assert "movie_id" in fact_crew_df.columns
    assert fact_crew_df["movie_id"].isnull().sum() == 0

def test_transform_genre_data(raw_data):
    """ Test transformation, Ensuring primary key exists and not null """
    dim_genre_df = transform_genre_data(raw_data)

    assert "genre_id" in dim_genre_df.columns
    assert dim_genre_df["genre_id"].isnull().sum() == 0