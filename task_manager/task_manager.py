from concurrent import futures
import threading
import time
import grpc


import proto.tasks_pb2_grpc 
import proto.tasks_pb2

from google.protobuf.timestamp_pb2 import Timestamp

class TaskManagerServicer(proto.tasks_pb2_grpc.TaskManagerServicer):
    def __init__(self):
        self.messages = []
        self.current_i = [0]
        self.lock=threading.Lock()
    
    def CreateTask(self, request, context):
        timestamp = Timestamp()
        timestamp.GetCurrentTime()
        return proto.tasks_pb2.TaskResponse()
    
    def UpdateTask(self, request, context):
        timestamp = Timestamp()
        timestamp.GetCurrentTime()
        return proto.tasks_pb2.TaskResponse()
    
    def DeleteTask(self, request, context):
        timestamp = Timestamp()
        timestamp.GetCurrentTime()
        return proto.tasks_pb2.TaskResponse()
    
    def GetMyTasks(self, request, context):
        timestamp = Timestamp()
        timestamp.GetCurrentTime()
        return proto.tasks_pb2.Tasks()

server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
proto.tasks_pb2_grpc.add_TaskManagerServicer_to_server(TaskManagerServicer(), server)
server.add_insecure_port("0.0.0.0:51075")
server.start()
server.wait_for_termination()