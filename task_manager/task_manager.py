from concurrent import futures
import logging
import os
import psycopg2
import grpc


import proto.tasks_pb2
import proto.tasks_pb2_grpc

from google.protobuf.timestamp_pb2 import Timestamp

dbname = os.environ.get('POSTGRES_DB', 'tasks_db')
user = os.environ.get('POSTGRES_USER', 'postgres')
password = os.environ.get('POSTGRES_PASSWORD', 'password123')
host = "postgres_tasks_db"


def connect_db():
    conn = psycopg2.connect(database=dbname, user=user, password=password, host=host)
    return conn

def fill_Task_proto(proto):
    logger.debug(f"kek01")
    if not proto.HasField("userId"):
        proto.userId = 0
    logger.debug(f"kek02")
    if not proto.HasField("content"):
        proto.content = ""
    if not proto.HasField("dateOfCreation"):
        timestamp = Timestamp()
        timestamp.GetCurrentTime()
        proto.dateOfCreation = timestamp
    if not proto.HasField("deadline"):
        proto.deadline = Timestamp()
    if not proto.HasField("status"):
        proto.status = 0
    return proto



class TaskManagerServicer(proto.tasks_pb2_grpc.TaskManagerServicer):
    def __init__(self):
        print()
    
    def CreateTask(self, request: proto.tasks_pb2.Task, context: grpc.ServicerContext) -> proto.tasks_pb2.TaskResponse:
        try:
            cursor = None
            conn = connect_db()
            cursor = conn.cursor()
            logger.debug(f"kek0")

            # request = fill_Task_proto(request)
            logger.debug(f"kek1")

            query = "INSERT INTO tasks (user_id, content, date_of_creation, deadline, status) VALUES (%d, '%s', '%s', '%s', %d);" % (request.userId, request.content, str(request.dateOfCreation), str(request.deadline), request.status)
            logger.debug(query)
            cursor.execute(query)
            logger.debug(f"kek2")

            query = "SELECT id FROM tasks WHERE user_id = %d AND content = '%s';" % (request.userId, request.content)
            cursor.execute(query) # another
            logger.debug(f"kek3")
            id = cursor.fetchone()[0]
            conn.commit()
            logger.debug(id)
            return proto.tasks_pb2.TaskResponse(id=id, status="Ok")
        except Exception as e:
            conn.rollback()
            return proto.tasks_pb2.TaskResponse(status=str(e))
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()
    
    def UpdateTask(self, request, context):
        try:
            cursor = None
            conn = connect_db()
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM tasks WHERE id = %s AND user_id = %s;", (request.id, request.task.userId,))
            task = cursor.fetchone()

            if not task:
                conn.close()
                return proto.tasks_pb2.TaskResponse(status=str("No such task")) # через id код возрврата

            update_data = {}
            if request.task.content != "":
                update_data['content'] = request.task.content
            if request.task.deadline != "":
                update_data['deadline'] = str(request.task.deadline)

            if len(update_data) > 0:
                update_query = ", ".join([f"{field} = '%s'" for field in update_data.keys()])
                values = list(update_data.values())
                values.append(request.id)

                prepare_query = f"UPDATE tasks SET {update_query} WHERE id = '%s';"
                logger.debug(prepare_query)
                logger.debug(values)
                query = prepare_query % tuple(values)
                logger.debug(query)
                cursor.execute(query)
            
            if request.task.status != "":
                prepare_query = f"UPDATE tasks SET status = %d WHERE id = '%s';"
                logger.debug(prepare_query)
                query = prepare_query % (request.task.status, request.id)
                logger.debug(query)
                cursor.execute(query)
            
            conn.commit()
            return proto.tasks_pb2.TaskResponse(id=request.id, status="Ok")
        except Exception as e:
            conn.rollback()
            return proto.tasks_pb2.TaskResponse(status=str(e))
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()
    
    def DeleteTask(self, request, context):
        try:
            cursor = None
            conn = connect_db()
            cursor = conn.cursor()
            logger.debug("kek1")

            cursor.execute("SELECT * FROM tasks WHERE id = %s AND user_id = %s;", (request.id, request.userId,))
            task = cursor.fetchone()
            logger.debug("kek2")

            if not task:
                conn.close()
                return proto.tasks_pb2.TaskResponse(status=str("No such task"), id=404) # через id код возрврата
            
            logger.debug("kek3")

            query = f"DELETE FROM tasks WHERE id = %s;" % (request.id)
            logger.debug(query)
            cursor.execute(query)
            
            conn.commit()
            return proto.tasks_pb2.TaskResponse(id=request.id, status="Ok")
        except Exception as e:
            conn.rollback()
            return proto.tasks_pb2.TaskResponse(status=str(e))
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()
    
    def GetTasks(self, request, context):
        try:
            cursor = None
            conn = connect_db()
            cursor = conn.cursor()
            logger.debug("kek1")

            tasks_proto = proto.tasks_pb2.Tasks()
            query = ""
            if request.userId != 0:
                query = "SELECT * FROM tasks WHERE user_id = %s ORDER BY id LIMIT %d OFFSET %d;" % (request.userId, request.size, request.offset)
            else:
                query = "SELECT * FROM tasks ORDER BY id LIMIT %d OFFSET %d;" % (request.size, request.offset)
            logger.debug(query)
            cursor.execute(query)
            logger.debug("kek22")
            rows = cursor.fetchall()
            logger.debug(len(rows))
            for row in rows:
                task = proto.tasks_pb2.Task()
                task.userId = int(row[1])
                task.content = row[2]
                logger.debug("kek2")

                if row[3]:
                    date_of_creation = Timestamp()
                    logger.debug(row[3])
                    date_of_creation = str(row[3])
                    logger.debug(date_of_creation)

                    # task.dateOfCreation.CopyFrom(date_of_creation) # TODO: fix conversion
                logger.debug("kek3")

                if row[4]:
                    deadline = Timestamp()
                    deadline = str(row[4])
                    # task.deadline.CopyFrom(deadline)
                logger.debug("kek4")

                task.status = int(row[5])
                logger.debug("kek5")
                logger.debug(task)
                tasks_proto.tasks.append(task)

            logger.debug(tasks_proto)
            
            return tasks_proto
        except Exception as e:
            conn.rollback()
            return proto.tasks_pb2.Tasks()
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

    def GetTaskById(self, request, context):
        try:
            cursor = None
            conn = connect_db()
            cursor = conn.cursor()
            logger.debug("kek1")

            if request.userId != 0:
                cursor.execute("SELECT * FROM tasks WHERE id = %s AND user_id = %s;", (request.id, request.userId,))
            else:
                cursor.execute("SELECT * FROM tasks WHERE id = %s;", (request.id,))
            
            task_row = cursor.fetchone()
            logger.debug("kek2")

            if not task_row:
                conn.close()
                return proto.tasks_pb2.Task(status= 404)

            task = proto.tasks_pb2.Task()
            task.userId = int(task_row[1])
            task.content = task_row[2]
            logger.debug("kek2")
            if task_row[3]:
                date_of_creation = Timestamp()
                logger.debug(task_row[3])
                date_of_creation = str(task_row[3])
                logger.debug(date_of_creation)
                logger.debug("lol")
                # task.dateOfCreation.CopyFrom(date_of_creation)
            logger.debug("kek3")
            if task_row[4]:
                deadline = Timestamp()
                deadline = str(task_row[4])
                # task.deadline.CopyFrom(deadline)
            logger.debug("kek4")
            task.status = int(task_row[5])
            
            return task
        except Exception as e:
            conn.rollback()
            return proto.tasks_pb2.TaskResponse(status=str(e))
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()


logging.basicConfig(level=logging.DEBUG)  # Установите уровень логирования
logger = logging.getLogger(__name__)
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
proto.tasks_pb2_grpc.add_TaskManagerServicer_to_server(TaskManagerServicer(), server)
server.add_insecure_port("0.0.0.0:51075")
server.start()
server.wait_for_termination()