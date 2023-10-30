from datetime import datetime

import backoff
from elasticsearch import Elasticsearch
from elasticsearch.helpers import BulkIndexError, bulk

from etl.config import ELASTICSEARCH_CONFIG, logger
from etl.es_schema import MAPPING_MOVIES
from etl.postgres import fetch_movie_data

INDEX_NAME = 'movies'

LAST_UPDATED_FILE = 'last_updated.txt'


def get_last_updated_time():
    try:
        with open(LAST_UPDATED_FILE, 'r') as f:
            return datetime.fromisoformat(f.read().strip())
    except (FileNotFoundError, ValueError):
        return datetime.min


def update_last_updated_time(last_updated_time):
    with open(LAST_UPDATED_FILE, 'w') as f:
        f.write(last_updated_time.isoformat())


@backoff.on_exception(backoff.expo, Exception, max_tries=8)
def create_index():
    es = Elasticsearch([ELASTICSEARCH_CONFIG])

    # Проверка, существует ли индекс
    if not es.indices.exists(index=INDEX_NAME):
        # Создание индекса с заданной схемой
        es.indices.create(index=INDEX_NAME, body=MAPPING_MOVIES)
        logger.info(f"Index '{INDEX_NAME}' created.")
    else:
        logger.info(f"Index '{INDEX_NAME}' already exists.")


@backoff.on_exception(backoff.expo, Exception, max_tries=8)
def write_to_elasticsearch(movie_ids):
    es = Elasticsearch([ELASTICSEARCH_CONFIG])
    # Преобразование списка идентификаторов фильмов в массив строк
    movie_ids = [str(movie_id).strip("['']") for movie_id in movie_ids]
    # Получение данных фильмов
    if movie_ids:
        movies, actors, directors, writers = fetch_movie_data(movie_ids)

        actions = []
        for movie in movies:
            action = {
                "_index": INDEX_NAME,
                "_id": movie["id"],
                "_source": {
                    "id": movie["id"],
                    "title": movie["title"],
                    "description": movie["description"],
                    "imdb_rating": movie["rating"],
                    "genre": movie["genres"],
                    "actors": [{"id": actor["id"], "name": actor["full_name"]} for actor in actors if
                               actor["film_work_id"] == movie["id"]],
                    "director": ", ".join([director["full_name"] for director in directors if
                                           director["film_work_id"] == movie["id"]]),
                    "writers": [{"id": writer["id"], "name": writer["full_name"]} for writer in writers if
                                writer["film_work_id"] == movie["id"]],
                    "writers_names": [writer["full_name"] for writer in writers if
                                      writer["film_work_id"] == movie["id"]],
                    "actors_names": [actor["full_name"] for actor in actors if actor["film_work_id"] == movie["id"]],
                },

            }
            actions.append(action)

        try:
            success, _ = bulk(es, actions)
            logger.info(f"Successfully indexed {success} movies.")
            update_last_updated_time(datetime.now())
        except BulkIndexError as e:
            logger.error("Failed to index documents:")
            for error in e.errors:
                logger.error(error)
    else:
        logger.info("No changes")
