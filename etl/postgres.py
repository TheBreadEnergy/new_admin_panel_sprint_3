import backoff
import psycopg2
from psycopg2.extras import DictCursor
from contextlib import closing

from etl.config import POSTGRES_CONFIG

FETCH_UPDATED_MOVIE_IDS_SQL = """
    SELECT id FROM content.film_work
    WHERE modified > %s
"""

FETCH_MOVIE_DATA_SQL = """
    SELECT fw.id, fw.title, fw.description, fw.rating,
        array_agg(g.name) as genres
    FROM content.film_work fw
    JOIN content.genre_film_work gfw ON fw.id = gfw.film_work_id
    JOIN content.genre g ON gfw.genre_id = g.id
    WHERE fw.id IN %s
    GROUP BY fw.id
"""

FETCH_ROLE_SQL = """
    SELECT pfw.film_work_id, p.id, p.full_name
    FROM content.person_film_work pfw
    JOIN content.person p ON pfw.person_id = p.id
    WHERE pfw.film_work_id IN %s AND pfw.role = %s
"""


@backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_tries=8)
def execute_sql_query(sql, params, fetch_size=100):
    with closing(psycopg2.connect(**POSTGRES_CONFIG)) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(sql, params)
            while True:
                records = cursor.fetchmany(fetch_size)
                if not records:
                    break
                for record in records:
                    yield record


def fetch_updated_movie_ids(last_updated_time):
    return execute_sql_query(FETCH_UPDATED_MOVIE_IDS_SQL, (last_updated_time,))


def fetch_movie_data(movie_ids):
    movies = execute_sql_query(FETCH_MOVIE_DATA_SQL, (tuple(movie_ids),))
    actors = execute_sql_query(FETCH_ROLE_SQL, (tuple(movie_ids), 'actor'))
    director = execute_sql_query(FETCH_ROLE_SQL, (tuple(movie_ids), 'director'))
    writer = execute_sql_query(FETCH_ROLE_SQL, (tuple(movie_ids), 'writer'))

    return list(movies), list(actors), list(director), list(writer)
