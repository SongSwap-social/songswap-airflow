"""Utility functions for transforming and loading listening history data to RDS."""
import logging
from typing import List, Set, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import connection, cursor
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def generate_bulk_insert_query(table: str, data: List[dict]) -> str:
    """Generate a SQL query for bulk insert operation.
    Args:
        table (str): The name of the table to insert into.
        data (List[dict]): The data to be inserted. It should be a list of dictionaries.
    Returns:
        str: SQL query string.
    """
    columns = data[0].keys()
    query = f"INSERT INTO \"{table}\" ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
    return query


def get_database_cursor(
    pg_hook: PostgresHook,
) -> Tuple[connection, cursor]:
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    return conn, cursor


def fetch_query_results_in_chunks(
    cursor: cursor, query: str, chunk_size: int = 1000
) -> Set[Tuple]:
    """
    Fetches query results in chunks, useful when working with large datasets to avoid memory issues.

    Args:
        cursor (cursor): The cursor object to execute the query.
        query (str): The SQL query string.
        chunk_size (int, optional): The size of each chunk to fetch at a time. Defaults to 1000.

    Returns:
        Set[Tuple]: A set of tuples, each representing a row in the query result.
    """
    queried_data = set()
    offset = 0
    while True:
        cursor.execute(query + f" LIMIT {chunk_size} OFFSET {offset}")
        results = cursor.fetchall()
        if not results:
            break
        queried_data.update(results)
        offset += chunk_size
    return queried_data


def execute_query_with_data(pg_cursor: cursor, query: str, data: List[dict]):
    """
    Executes a given SQL query with a list of data.

    Committing the transaction is the responsibility of the caller.

    Args:
        pg_cursor (cursor): The cursor object to execute the query.
        query (str): The SQL query string.
        data (List[dict]): The data to be inserted/updated. Each dict in the list represents a row of data.
    """
    tuple_data = [tuple(item.values()) for item in data]
    execute_values(pg_cursor, query, tuple_data)


def insert_bulk(transformed_data: dict, pg_hook: PostgresHook):
    """Insert multiple rows of data into the database.
    Args:
        transformed_data (dict): A dictionary of table names mapped to a list of data to be inserted.
            Each dict in the list represents a row of data, and the keys of the dict represent the column names.
        pg_hook (PostgresHook): Hook to connect to the database.
    """

    # Start a new transaction
    conn, cursor = get_database_cursor(pg_hook)

    logger.info(f"Inserting data into {len(transformed_data)} tables.")
    try:
        for table, table_data in transformed_data.items():
            if table_data:
                query = generate_bulk_insert_query(table, table_data)
                execute_query_with_data(cursor, query, table_data)
        # Commit the transaction after all inserts
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert data: {str(e)}")
        raise e
    finally:
        cursor.close()
        conn.close()
