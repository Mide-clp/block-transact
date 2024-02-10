import psycopg2
from psycopg2.extras import execute_batch
import datetime


def establish_connection(host, port, db, user, password):
    """
    Establish a connection to a PostgreSQL database.

    :param host: PostgreSQL host address
    :param port:  PostgreSQL port number. Default is 5432.
    :param db:  PostgreSQL database name.
    :param user:  PostgreSQL database user.
    :param password: PostgreSQL database password.
    :return: Cursor for executing SQL queries.
    """

    # connection for pandas
    # engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    # engine.connect()
    cur = None
    # connection for psycopg2 to postgres
    try:
        conn = psycopg2.connect(host=host, database=db, user=user, password=password, port=port)
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        print(f"connected to {db} database")
    except psycopg2.Error as e:
        print("error creating connection")
        raise e

    return cur


def run_sql_query(cur, query):
    """
    Execute a SQL query using the provided cursor.

    :param cur: Cursor for executing SQL queries.
    :param query: SQL query to be executed.
    :return:
    """
    try:
        cur.execute(query)
        print(f"query executed successfully")
    except psycopg2.Error as e:
        print(f"Error running: {query}")
        print(e)


def write_batch_to_db(cur, query: str, data: list[dict]) -> None:
    """
    This function uses psycopg2's `execute_batch` method to efficiently write a batch of data to the database.

    :param cur: Cursor for executing SQL queries.
    :param query: SQL query for the batch insert.
    :param data: List of dictionaries, where each dictionary represents a row of data to be inserted.
    :return:
    """
    start = datetime.datetime.now()
    try:
        execute_batch(cur, query, data, page_size=1000)
        # conn.commit()
        print("batch insert complete")
    except psycopg2.Error as e:
        print("error inserting to table")
        print(e)
    else:
        end = datetime.datetime.now() - start
        print(f"Time taken: {end}")
