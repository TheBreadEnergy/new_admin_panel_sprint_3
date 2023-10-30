import psycopg2
from psycopg2.extras import DictCursor
import backoff

from etl.config import POSTGRES_CONFIG


@backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_tries=8)
def execute_sql_query(sql, params):
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor(cursor_factory=DictCursor)
    cursor.execute(sql, params)
    result = cursor.fetchall()
    conn.close()
    return result


def fetch_updated_movie_ids(last_updated_time):
    sql = """
        SELECT id FROM content.film_work
        WHERE modified > %s
    """
    return execute_sql_query(sql, (last_updated_time,))


def fetch_movie_data(movie_ids):
    movie_sql = """
        SELECT fw.id, fw.title, fw.description, fw.rating,
               array_agg(g.name) as genres
        FROM content.film_work fw
        JOIN content.genre_film_work gfw ON fw.id = gfw.film_work_id
        JOIN content.genre g ON gfw.genre_id = g.id
        WHERE fw.id IN %s
        GROUP BY fw.id
    """

    role_sql = """
        SELECT pfw.film_work_id, p.id, p.full_name
        FROM content.person_film_work pfw
        JOIN content.person p ON pfw.person_id = p.id
        WHERE pfw.film_work_id IN %s AND pfw.role = %s
    """

    movies = execute_sql_query(movie_sql, (tuple(movie_ids),))
    actors = execute_sql_query(role_sql, (tuple(movie_ids), 'actor'))
    director = execute_sql_query(role_sql, (tuple(movie_ids), 'director'))
    writer = execute_sql_query(role_sql, (tuple(movie_ids), 'writer'))

    return movies, actors, director, writer
