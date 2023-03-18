import os
import pymysql
import mysql.connector
import time
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

host = os.environ['RDS_HOST_NAME']
user = os.environ['USER']
password = os.environ['PASS']
db = os.environ['DB']
connect_count = os.environ['CONNECT_COUNT']
connect_timeout = os.environ['CONNECT_TIMEOUT']


def lambda_handler(event, context):
    # _connect()
    _multi_connect()
    return {
        'isBase64Encoded': False,
        'statusCode': 200,
        'headers': {},
        'body': '{"message": "Hello from AWS Lambda"}'
    }


def _multi_connect():
    # pymysql
    conns = [pymysql.connect(host=host, user=user, password=password, database=db, connect_timeout=int(connect_timeout)) for i in range(int(connect_count))]
    time.sleep(60)

    # mysql-connector-python
    # conns = [mysql.connector.connect(host=host, user=user, password=password, database=db, connect_timeout=int(connect_timeout)) for i in range(int(connect_count))]

    for conn in conns:
        with conn.cursor() as cursors:
            # cursors.execute('lock tables user read')
            cursors.execute('show databases')
            print(cursors.fetchall())
        conn.close()


def _connect():
    # sqlalchemy
    engine = create_engine(
        'mysql+pymysql://{user}:{password}@{host}/{dbname}'.format(user=user, password=password, host=host, dbname=db),
        echo=True,
        poolclass=NullPool,
        # pool_size=1,
    )

    session = scoped_session(
        sessionmaker(
            autocommit=False,
            autoflush=True,
            expire_on_commit=False,
            bind=engine
        )
    )
    # with engine.connect() as conn:
    res = session.execute(text('select now()'))
    for v in res:
        print(v)

    session.close()
