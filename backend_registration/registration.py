import logging
import os
from flask import Flask, request, jsonify
import psycopg2

import grpc
import google.protobuf.json_format
import google.protobuf.empty_pb2
import google.protobuf.json_format as json_format


from proto import tasks_pb2
from proto import tasks_pb2_grpc 

import util

app = Flask(__name__)

dbname = os.environ.get('POSTGRES_DB', 'main')
user = os.environ.get('POSTGRES_USER', 'postgres')
password = os.environ.get('POSTGRES_PASSWORD', 'password123')
host = "postgres_users_db"


def connect_db():
    conn = psycopg2.connect(database=dbname, user=user, password=password, host=host)
    return conn

def get_user_id_by_token(token):
    try:
        cursor = None
        conn = connect_db()
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM users WHERE token = %s;", (token,))
        user = cursor.fetchone()
        if user:
            user_id = user[0]
            return user_id
    except Exception as e:
        conn.rollback()
        return "-1"
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

@app.route('/register', methods=['POST'])
def register_user():
    data = request.json
    if not data.get('username') or not data.get('password'):
        return (
            jsonify({'message': 'Missing login or password.'}),
            404
        )

    username = data.get('username')
    hashed_password = util.hash_password(data.get('password'))

    try:
        cur = None
        conn = connect_db()
        cur = conn.cursor()

        app.logger.debug(data['username'])
        app.logger.debug(type(data['username']))
        
        cur.execute("SELECT * FROM users WHERE username = %s;", (username,))
        user = cur.fetchone()
        if user:
            conn.close()
            return jsonify({'message': 'User already exist'}), 400
        else:
            cur.execute("INSERT INTO users (username, password) VALUES (%s, %s);", (username, hashed_password))
            conn.commit()
            conn.close()
            return jsonify({'message': 'User successfully registered'})
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

@app.route('/update', methods=['PUT'])
def update_user():
    data = request.json
    token = request.headers.get('token')
    
    if not token:
        return jsonify({'message': 'Token is missing'}), 400

    try:
        cursor = None
        conn = connect_db()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM users WHERE token = %s;", (token,))
        user = cursor.fetchone()

        if not user:
            conn.close()
            return jsonify({'message': 'Invalid token'}), 401

        update_data = {}
        fields = ['first_name', 'last_name', 'date_of_birth', 'email', 'phone_number']
        for field in fields:
            if data.get(field):
                update_data[field] = data[field]

        if len(update_data) > 0:
            update_query = ", ".join([f"{field} = '%s'" for field in update_data.keys()])
            values = list(update_data.values())
            values.append(token)

            prepare_query = f"UPDATE users SET {update_query} WHERE token = '%s';"
            app.logger.debug(prepare_query)
            query = prepare_query % (values[0], values[1])
            app.logger.debug(query)
            cursor.execute(query)
            conn.commit()
            return jsonify({'message': 'User information successfully updated'})
        else:
            return jsonify({'message': 'Nothing to update'}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


@app.route('/authenticate', methods=['POST'])
def authenticate_user():
    data = request.json
    if not data.get('username') or not data.get('password'):
        return (
            jsonify({'message': 'Missing login or password.'}),
            404
        )
    username = data.get('username')
    password = data.get('password')
    
    try:
        cursor = None
        conn = connect_db()
        cursor = conn.cursor()

        hashed_password = util.hash_password(password)

        cursor.execute("SELECT * FROM users WHERE username = %s AND password = %s;", (username, hashed_password,))
        user = cursor.fetchone()

        if user:
            token = util.generate_token()
            cursor.execute("UPDATE users SET token = %s WHERE username = %s;", (token, username,))
            conn.commit()
            conn.close()
            return jsonify({'message': 'User authenticated successfully', 'token': token})
        else:
            conn.close()
            return jsonify({'message': 'User does not exist or password is wrong'}), 401
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

@app.route('/create_task', methods=['POST'])
def create_task():
    data = request.json
    token = request.headers.get('token')
    if not data.get('description') or not token:
        return (
            jsonify({'message': 'Missing description field or token'}),
            404
        )
    content = data.get('description', "")
    deadline = data.get('deadline', "1970-01-01T01:00:00.000Z")
    app.logger.debug("kek0")
    
    try:
        user_id = get_user_id_by_token(token)
        if not user_id:
            return jsonify({'error': "No user tith such token, authenticate one more time"}), 400
        json_data = {
            "userId": user_id,
            "content": content,
            "deadline": deadline,
            "status": 0
        }
        # deadline format 2018-03-07T01:00:00.000Z

        app.logger.debug("kek1")

        proto_message=google.protobuf.json_format.ParseDict(json_data, tasks_pb2.Task())
        grpc_server_address = os.environ.get('GRPC_TASKS_SERVER_ADDR', 'localhost:51075')

        app.logger.debug(proto_message)
        app.logger.debug(grpc_server_address)

        channel = grpc.insecure_channel(grpc_server_address)
        stub = tasks_pb2_grpc.TaskManagerStub(channel)
        app.logger.debug("kek3")
        response = stub.CreateTask(proto_message)
        app.logger.debug("kek4")
        res =  google.protobuf.json_format.MessageToDict(response)
        app.logger.debug("kek5")
        app.logger.debug(res)
        return res
    except Exception as e:
        app.logger.debug(str(e))
        return jsonify({'error': str(e)}), 500

@app.route('/update_task', methods=['PUT'])
def update_task():
    data = request.json
    token = request.headers.get('token')
    if not data.get('id') or not token:
        return (
            jsonify({'message': 'Missing id of the task or token'}),
            404
        )
    id = data.get('id', 0)
    content = data.get('content', "")
    deadline = data.get('deadline', "1970-01-01T01:00:00.000Z")
    status = data.get('status', 0)
    app.logger.debug("kek0")
    
    try:
        user_id = get_user_id_by_token(token)
        if not user_id:
            return jsonify({'error': "No user tith such token, authenticate one more time"}), 400
        
        proto = tasks_pb2.TaskWithId()
        json_data = {
            "id": int(id),
            "task": {
                "userId": user_id,
                "content": content,
                "deadline": deadline,
                "status": int(status)
            }
        }
        # deadline format 2018-03-07T01:00:00.000Z

        app.logger.debug("kek1 " + content)
        app.logger.debug(json_data)

        json_format.ParseDict(json_data, proto, ignore_unknown_fields=True)

        app.logger.debug(proto)
        # proto.id = id
        # proto.task.userId = task_proto.userId
        # proto.task.content = task_proto.content
        # proto.task.deadline = task_proto.deadline
        # proto.task.status = task_proto.status
        grpc_server_address = os.environ.get('GRPC_TASKS_SERVER_ADDR', 'localhost:51075')

        app.logger.debug("kek2")
        app.logger.debug(grpc_server_address)

        channel = grpc.insecure_channel(grpc_server_address)
        stub = tasks_pb2_grpc.TaskManagerStub(channel)
        app.logger.debug("kek3")
        response = stub.UpdateTask(proto)
        app.logger.debug("kek4")
        res = google.protobuf.json_format.MessageToDict(response)
        app.logger.debug("kek5")
        app.logger.debug(res)
        return res
    except Exception as e:
        app.logger.debug(str(e))
        return jsonify({'error': str(e)}), 500

@app.route('/delete_task', methods=['DELETE'])
def delete_task():
    data = request.json
    token = request.headers.get('token')
    if not data.get('id') or not token:
        return (
            jsonify({'message': 'Missing id of the task or token'}),
            404
        )
    id = data.get('id', 0)

    app.logger.debug("kek0")
    
    try:
        user_id = get_user_id_by_token(token)
        if not user_id:
            return jsonify({'error': "No user tith such token, authenticate one more time"}), 400
        
        proto = tasks_pb2.TaskWithId()
        json_data = {
            "id": int(id),
            "task": {
                "userId": user_id
            }
        }

        app.logger.debug(json_data)

        json_format.ParseDict(json_data, proto, ignore_unknown_fields=True)

        app.logger.debug(proto)

        grpc_server_address = os.environ.get('GRPC_TASKS_SERVER_ADDR', 'localhost:51075')

        app.logger.debug("kek2")
        app.logger.debug(grpc_server_address)

        channel = grpc.insecure_channel(grpc_server_address)
        stub = tasks_pb2_grpc.TaskManagerStub(channel)
        app.logger.debug("kek3")
        response = stub.DeleteTask(proto)
        app.logger.debug("kek4")
        res = google.protobuf.json_format.MessageToDict(response)
        app.logger.debug("kek5")
        app.logger.debug(res)
        return res
    except Exception as e:
        app.logger.debug(str(e))
        return jsonify({'error': str(e)}), 500

@app.route('/get_my_tasks', methods=['GET'])
def get_my_tasks():
    token = request.headers.get('token')
    if not token:
        return (
            jsonify({'message': 'Missing token'}),
            404
        )

    app.logger.debug("kek0")
    
    try:
        user_id = get_user_id_by_token(token)
        if not user_id:
            return jsonify({'error': "No user tith such token, authenticate one more time"}), 400
        
        proto = tasks_pb2.TaskWithId()
        json_data = {
            "task": {
                "userId": user_id
            }
        }

        app.logger.debug(json_data)

        json_format.ParseDict(json_data, proto, ignore_unknown_fields=True)

        app.logger.debug(proto)

        grpc_server_address = os.environ.get('GRPC_TASKS_SERVER_ADDR', 'localhost:51075')

        app.logger.debug("kek2")
        app.logger.debug(grpc_server_address)

        channel = grpc.insecure_channel(grpc_server_address)
        stub = tasks_pb2_grpc.TaskManagerStub(channel)
        app.logger.debug("kek3")
        response = stub.GetMyTasks(proto)
        app.logger.debug("kek4")
        res = google.protobuf.json_format.MessageToDict(response)
        app.logger.debug("kek5")
        app.logger.debug(res)
        return res
    except Exception as e:
        app.logger.debug(str(e))
        return jsonify({'error': str(e)}), 500


@app.route('/get_task_by_id', methods=['GET'])
def get_task_by_id():
    data = request.json
    token = request.headers.get('token')
    if not data.get('id') or not token:
        return (
            jsonify({'message': 'Missing id of the task or token'}),
            404
        )
    id = data.get('id', 0)
    
    try:
        user_id = get_user_id_by_token(token)
        if not user_id:
            return jsonify({'error': "No user tith such token, authenticate one more time"}), 400
        
        proto = tasks_pb2.TaskWithId()
        json_data = {
            "id": int(id),
            "task": {
                "userId": user_id
            }
        }

        app.logger.debug(json_data)

        json_format.ParseDict(json_data, proto, ignore_unknown_fields=True)

        app.logger.debug(proto)

        grpc_server_address = os.environ.get('GRPC_TASKS_SERVER_ADDR', 'localhost:51075')

        app.logger.debug("kek2")
        app.logger.debug(grpc_server_address)

        channel = grpc.insecure_channel(grpc_server_address)
        stub = tasks_pb2_grpc.TaskManagerStub(channel)
        app.logger.debug("kek3")
        response = stub.GetTaskById(proto)
        app.logger.debug("kek4")
        res = google.protobuf.json_format.MessageToDict(response)
        app.logger.debug("kek5")
        app.logger.debug(res)
        return res
    except Exception as e:
        app.logger.debug(str(e))
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    logger = logging.getLogger("server")
    logger.setLevel(logging.DEBUG)
    app.run(debug=True, port=5001, host='0.0.0.0')
