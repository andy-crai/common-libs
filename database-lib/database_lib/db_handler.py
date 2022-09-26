import logging
import copy

from tenacity import retry, stop_after_delay, stop_after_attempt, wait_fixed
from psycopg2.extras import execute_values as ps_execute_values
from psycopg2 import DatabaseError, OperationalError, pool

# from abc import ABC, abstractmethod


@retry(stop=(stop_after_delay(10) | stop_after_attempt(5)), wait=wait_fixed(1))
def init_connection_pool(host, pw, db, user, min_conn, max_conn):
    logging.info("Initializing connection pool %s, %s", host, db)
    connectionPool = pool.ThreadedConnectionPool(
        minconn=min_conn,
        maxconn=max_conn,
        dbname=db,
        user=user,
        host=host,
        password=pw,
        connect_timeout=3,
        keepalives=10,
        keepalives_idle=20,
        keepalives_interval=2,
        options="",
        keepalives_count=3,
    )
    logging.info("connected to db %s", host)
    return connectionPool


class ConnectionPoolManager():

    def __init__(self, connection_helper, min_conn, max_conn) -> None:
        self.connection_pool_dict = {}
        self.connection_helper = connection_helper
        self.min_conn = min_conn
        self.max_conn = max_conn

    def get_connection_pool(self):
        key = self.connection_helper.get_key()
        connection_pool = self.connection_pool_dict.get(key)
        if connection_pool is None:
            db_data = self.connection_helper.get_db_data()
            connection_pool = init_connection_pool(host=db_data.host, pw=db_data.pw,
                                                   db=db_data.db, user=db_data.user,
                                                   min_conn=self.min_conn, max_conn=self.max_conn)
            self.connection_pool_dict[key] = connection_pool
        return connection_pool


connection_pool_manager: ConnectionPoolManager = None


def init_connection_pool_manager(conn_pool_manager):
    global connection_pool_manager
    connection_pool_manager = conn_pool_manager
    logging.info(f"connection pool initialized {connection_pool_manager}")


class DatabaseManager:

    def __init__(self, connection) -> None:
        self.connection = connection
        self.cursor = self.connection.cursor()

    def execute_query(self, query, params=None):
        self.cursor.execute(query, params)
        return self.cursor

    def execute_values(self, query, params=None):
        ps_execute_values(cur=self.cursor, sql=query, argslist=params)
        return self.cursor

    def getconn(self):
        if self.connection:
            logging.info("connection is not null")
            return self.connection
        else:
            logging.info("connection is none")
            return None


class CCursor:
    """Custom cursor that wraps the psycopg2 cursor"""

    def __init__(self, cursor) -> None:
        self.cursor = cursor

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchmany(self):
        return self.cursor.fetchmany()

    def getconn(self):
        return self.cursor.connection


class DatabaseException(Exception):
    """Base class for database exceptions"""

    def __init__(self, message, errors) -> None:
        super().__init__(message)
        logging.error("Error executing query %s", message)


def transaction(func):
    """
    Creates a transaction and manages the connection rollback or commit.
    Handles database errors and gracefully manages the connection.
    """

    def wrapper(*args, **kwargs):
        logging.debug("transaction started")
        connection_pool = connection_pool_manager.get_connection_pool()
        connection = connection_pool.getconn()
        db_manager = DatabaseManager(connection)
        logging.debug("connection %s", str(connection))

        try:
            new_kwargs = copy.deepcopy(kwargs)
            if "db_manager" in new_kwargs:
                new_kwargs.pop("db_manager")
            if "retry" in new_kwargs:
                new_kwargs.pop("retry")
            ret = func(*args, **new_kwargs, db_manager=db_manager)
            logging.debug("return data %s", str(ret))
            connection.commit()
            connection_pool.putconn(connection)
            logging.debug("transaction ended")
            return ret
        except OperationalError as opErr:
            logging.error("Operational error occured")
            connection_pool.putconn(conn=connection, close=True)
            retry_val = kwargs.get('retry', 0)
            kwargs['retry'] = retry_val+1
            logging.error(f"retry count {retry_val}")
            if retry_val > 10:
                raise DatabaseException("Error while handling request", errors=[opErr])
            return wrapper(*args, **kwargs)
        except DatabaseError as err:
            logging.error("Error executing sql %s", str(err))
            connection.rollback()
            connection_pool.putconn(connection)
            raise DatabaseException("Error while handling request", errors=[err])
        except BaseException as baseErr:
            logging.error("Error executing sql %s", str(baseErr))
            connection.rollback()
            connection_pool.putconn(connection)
            raise baseErr

    return wrapper
