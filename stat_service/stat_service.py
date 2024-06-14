import logging
import multiprocessing
import os
from kafka import KafkaConsumer
import time
import json
import psycopg2

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# database util

dbname = os.environ.get('POSTGRES_DB', 'statistics_db')
user = os.environ.get('POSTGRES_USER', 'postgres')
password = os.environ.get('POSTGRES_PASSWORD', 'password123')
host = "postgres_statistics_db"

def connect_db():
    conn = psycopg2.connect(database=dbname, user=user, password=password, host=host)
    return conn

def write_comment_info_to_db(comment):
    try:
        cursor = None
        conn = connect_db()
        cursor = conn.cursor()
        logger.debug(f"kek0")

        query = "INSERT INTO comments (task_id, user_id, content) VALUES (%d, %d, '%s');" % (comment.get('task_id'), comment.get('user_id'), comment.get('comment'))
        logger.debug(query)
        cursor.execute(query)
        conn.commit()
        return True
    except Exception as e:
        return False
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def write_like_info_to_db(like):
    try:
        cursor = None
        conn = connect_db()
        cursor = conn.cursor()
        logger.debug(f"kek0")

        query = "INSERT INTO likes (task_id, user_id) VALUES (%d, %d);" % (like.get('task_id'), like.get('user_id'))
        logger.debug(query)
        cursor.execute(query)
        conn.commit()
        return True
    except Exception as e:
        return False
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()



def create_kafka_comments_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                'comments',
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            return consumer
        except Exception:
            time.sleep(5)

def create_kafka_likes_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                'likes',
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            return consumer
        except Exception:
            time.sleep(5)

comments_consumer = create_kafka_comments_consumer()
likes_consumer = create_kafka_likes_consumer()

def comments_consume():
    for message in comments_consumer:
        logger.debug(f'Received comments message: {message.value}')
        status = write_comment_info_to_db(message.value)

def likes_consume():
    for message in likes_consumer:
        logger.debug(f'Received likes message: {message.value}') # print-ы - логи не работают
        status = write_like_info_to_db(message.value)



p1 = multiprocessing.Process(target=comments_consume)
p2 = multiprocessing.Process(target=likes_consume)

p1.start()
p2.start()

p1.join()
p2.join()



