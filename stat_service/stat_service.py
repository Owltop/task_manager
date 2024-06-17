import logging
from concurrent import futures
import multiprocessing
import os
from kafka import KafkaConsumer
import time
import json
import psycopg2
import grpc

import proto.tasks_statistics_pb2
import proto.tasks_statistics_pb2_grpc

from google.protobuf.empty_pb2 import Empty

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

        query = "SELECT COUNT(*) FROM comments WHERE task_id = %d AND user_id = %d and content = '%s';" % (comment.get('task_id'), comment.get('user_id'), comment.get('comment'))
        cursor.execute(query)
        if int(cursor.fetchone()[0]) == 0:
            query = "INSERT INTO comments (task_id, user_id, content) VALUES (%d, %d, '%s');" % (comment.get('task_id'), comment.get('user_id'), comment.get('comment'))
            logger.debug(query)
            cursor.execute(query)
            conn.commit()
        return True
    except Exception as e:
        logger.debug("Error: " + str(e))
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

        query = "SELECT COUNT(*) FROM likes WHERE task_id = %d AND user_id = %d;" % (like.get('task_id'), like.get('user_id'))
        cursor.execute(query)
        logger.debug("kek1")
        if int(cursor.fetchone()[0]) == 0:
            logger.debug("kek2")
            query = "INSERT INTO likes (task_id, user_id) VALUES (%d, %d);" % (like.get('task_id'), like.get('user_id'))
            logger.debug(query)
            cursor.execute(query)
            conn.commit()
        return True
    except Exception as e:
        logger.debug("Error: " + str(e))
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

class StatisticsManagerServicer(proto.tasks_statistics_pb2_grpc.StatisticsManagerServicer):
    def __init__(self):
        print()

    def GetTaskStatistics(self, request, context):
        try:
            task_id = request.id
            cursor = None
            conn = connect_db()
            cursor = conn.cursor()

            query = "SELECT COUNT(*) FROM likes WHERE task_id = %d;" % (task_id)
            logger.debug(query)
            cursor.execute(query)

            count_of_likes = cursor.fetchone()[0]

            query = "SELECT COUNT(*) FROM comments WHERE task_id = %d;" % (task_id)
            logger.debug(query)
            cursor.execute(query)

            count_of_comments = cursor.fetchone()[0]

            return proto.tasks_statistics_pb2.TaskStatisticsResponse(id=task_id, likes=count_of_likes, comments=count_of_comments)
        except Exception as e:
            return proto.tasks_statistics_pb2.TaskStatisticsResponse(status=str(e))
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

    def GetTop5Tasks(self, request, context):
        try:
            parameter = request.parameter

            cursor = None
            conn = connect_db()
            cursor = conn.cursor()

            array_of_tasks_statistics = proto.tasks_statistics_pb2.ArrayOfTasksStatistics() 
            if parameter == 'likes':
                cursor.execute(""" SELECT task_id, COUNT(*)
                                as like_count FROM likes
                                GROUP BY task_id 
                                ORDER BY like_count DESC LIMIT 5; """) 
                top_tasks = cursor.fetchall() 

                for task in top_tasks: 
                    one_task_statistics = proto.tasks_statistics_pb2.TaskStatisticsResponse() 
                    one_task_statistics.id = int(task[0]) 
                    one_task_statistics.likes = int(task[1]) 
                    array_of_tasks_statistics.tasks.append(one_task_statistics) 
            elif parameter == 'comments':
                cursor.execute(""" SELECT task_id, COUNT(*)
                                as comment_count FROM comments
                                GROUP BY task_id 
                                ORDER BY comment_count DESC LIMIT 5; """) 
                top_tasks = cursor.fetchall() 

                for task in top_tasks: 
                    one_task_statistics = proto.tasks_statistics_pb2.TaskStatisticsResponse() 
                    one_task_statistics.id = int(task[0]) 
                    one_task_statistics.comments = int(task[1]) 
                    array_of_tasks_statistics.tasks.append(one_task_statistics) 
            return array_of_tasks_statistics 
        except Exception as e: 
            return proto.tasks_statistics_pb2.ArrayOfTasksStatistics(status=str(e)) 
        finally: 
            if cursor is not None: 
                cursor.close() 
            if conn is not None: 
                conn.close()
    
    def GetTop3Users(self, request, context):
        try:
            cursor = None
            conn = connect_db()
            cursor = conn.cursor()

            array_of_users_likes_statistics = proto.tasks_statistics_pb2.ArrayOfUsersLikesStatistics() 
            cursor.execute(""" SELECT user_id, COUNT(*)
                            as user_count FROM likes
                            GROUP BY user_id 
                            ORDER BY user_count DESC LIMIT 5; """) 
            top_users = cursor.fetchall() 
            for user in top_users:
                user_statistics = proto.tasks_statistics_pb2.UserLikeStatistics() 

                user_statistics.userId = int(user[0])
                user_statistics.likesCount = int(user[1])
                array_of_users_likes_statistics.userStatistics.append(user_statistics)

            return array_of_users_likes_statistics 
        except Exception as e: 
            return proto.tasks_statistics_pb2.ArrayOfUsersLikesStatistics(status=str(e)) 
        finally: 
            if cursor is not None: 
                cursor.close() 
            if conn is not None: 
                conn.close()


server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
proto.tasks_statistics_pb2_grpc.add_StatisticsManagerServicer_to_server(StatisticsManagerServicer(), server)
server.add_insecure_port("0.0.0.0:51075")
server.start()
server.wait_for_termination()

p1.join()
p2.join()
