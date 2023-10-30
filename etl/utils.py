from etl.elasticsearch import (create_index, get_last_updated_time,
                               write_to_elasticsearch)
from etl.postgres import fetch_updated_movie_ids


def execute_etl():
    last_updated_time = get_last_updated_time()
    movie_ids = fetch_updated_movie_ids(last_updated_time)

    create_index()
    write_to_elasticsearch(movie_ids)
