import sqlite3
import pandas as pd
import numpy as np
import json
from elasticsearch import Elasticsearch, helpers
from typing import List
import uuid

QUERY_MOVIES = """WITH x as (
                        SELECT  m.id, 
                                group_concat(a.id) as actors_ids, 
                                group_concat(a.name) as actors_names
                        FROM movies m
                        LEFT JOIN movie_actors ma on m.id = ma.movie_id
                        LEFT JOIN actors a on ma.actor_id = a.id
                        GROUP BY m.id
                    )
                    SELECT  m.id, 
                            genre, 
                            director, 
                            title, 
                            plot, 
                            imdb_rating, 
                            x.actors_ids, 
                            x.actors_names,
                            CASE
                                WHEN m.writers = '' THEN '[{"id": "' || m.writer || '"}]'
                                ELSE m.writers
                            END AS writers
                    FROM movies m
                    LEFT JOIN x ON m.id = x.id
                   """

QUERY_WRITERS = """SELECT DISTINCT(ID), name FROM writers"""
DB = '<path to db sqlite>'


def get_columns(conn: str, query: str) -> List[str]:
    conn = sqlite3.connect(conn)
    cursor = conn.cursor()

    cursor.execute(query)
    columns = cursor.description

    headers = []
    for item in columns:
        headers.append(item[0])
    return headers


def raw_data_to_df(conn: str, query: str) -> List[{}]:
    conn = sqlite3.connect(conn)
    cursor = conn.cursor()

    # Извлекаем необходимые данные
    cursor.execute(query)

    data = cursor.fetchall()
    return data


def bulk_json_data(df, _index: str, doc_type: str):
    dx = df.to_json(orient='records')
    json_data = json.loads(dx)

    for doc in json_data:
        # use a `yield` generator so that the data
        # isn't loaded into memory
        if '{"index"' not in doc:
            yield {
                "_index": _index,
                "_type": doc_type,
                "_id": uuid.uuid4(),
                "_source": doc
            }


def load_to_es(json_data):
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    try:
        # make the bulk call, and get a response
        response = helpers.bulk(es, json_data)
        print("\nbulk_json_data() RESPONSE:", response)
    except Exception as e:
        print("\nERROR:", e)


if __name__ == '__main__':
    movies = pd.DataFrame(raw_data_to_df(DB, QUERY_MOVIES), columns=get_columns(DB, QUERY_MOVIES))
    movies['plot'].replace('N/A', np.nan, inplace=True)
    movies['director'].replace('N/A', np.nan, inplace=True)
    movies['writers'].replace('N/A', np.nan, inplace=True)
    movies['imdb_rating'].replace('N/A', np.nan, inplace=True)
    writers = pd.DataFrame(raw_data_to_df(DB, QUERY_WRITERS), columns=get_columns(DB, QUERY_WRITERS))

    load_to_es(bulk_json_data(movies, 'movies', 'movie'))
    load_to_es(bulk_json_data(writers, 'writers', 'writer'))
