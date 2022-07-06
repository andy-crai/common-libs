import logging

import psycopg2
from tenacity import retry, stop_after_delay, stop_after_attempt, wait_fixed
import os 

from psycopg2.extras import execute_values as ps_execute_values
from psycopg2 import  DatabaseError, pool


@retry(stop=(stop_after_delay(10) | stop_after_attempt(5)), wait=wait_fixed(1))
def init_connection_pool():
    host = os.getenv('host')
    pw = os.getenv('pwd')
    db = os.getenv('db')
    user = os.getenv('user')
    minconn = os.getenv('min_conn')
    maxconn = os.getenv('max_conn')
    global connectionPool
    connectionPool = pool.ThreadedConnectionPool(minconn=minconn, maxconn=maxconn,
        dbname=db,
        user=user,
        host=host,
        password=pw,
        connect_timeout=3,
        keepalives=1,
        keepalives_idle=5,
        keepalives_interval=2,
        options="",
        keepalives_count=2)
    logging.info("connected to db")
    return connectionPool

def execute_query(query, params, cur = None):
    if not cur:
        cur = connectionPool.getconn().cursor()
    cur.execute(query, params)
    return cur
    
    
def execute_values(query, params, cur = None):
    if not cur:
        cur = connectionPool.getconn().cursor()
    ps_execute_values(cur=cur, sql=query, argslist=params)
    return cur


connectionPool = init_connection_pool()

class CCursor():
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
        connection = connectionPool.getconn()
        try:
            ret = func(*args, **kwargs, cursor = connection.cursor())
            connection.commit()
            connectionPool.putconn(connection)
            logging.debug("transaction ended")
            return ret
        except DatabaseError as err:
            connection.rollback()
            logging.error("Error executing sql %s", str(err))
            connectionPool.putconn(connection)
            raise DatabaseException("Error while handling request")
        
    return wrapper
 