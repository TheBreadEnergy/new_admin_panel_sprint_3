from etl.postgres import fetch_updated_movie_ids
from etl.elasticsearch import create_index, write_to_elasticsearch, get_last_updated_time, update_last_updated_time


def execute_etl():
    last_updated_time = get_last_updated_time()
    movie_ids = fetch_updated_movie_ids(last_updated_time)

    create_index()
    write_to_elasticsearch(movie_ids)


