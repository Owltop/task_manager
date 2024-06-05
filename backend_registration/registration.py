import logging
import os
import sqlite3
from flask import Flask, request, jsonify
import psycopg2

import util

app = Flask(__name__)
db_path = '../data/users.db' # По факту обычный файлик, если он будет лежать в отдельном контейнере, то 1) как к нему получить доступ из другого контейнера? 2) Как сделаь так, чтобы контейнер с бд работал бесконечно(с костылями поянтно как)

dbname = "main"
user = "postgres"
password = "password123"
host = "postgres-users_db"


# Функция для подключения к базе данных
def connect_db():
    conn = psycopg2.connect(database=dbname, user=user, password=password, host=host)
    return conn

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

        # app.logger.debug(data['username'])
        # app.logger.debug(type(data['username']))
        
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
    token = data.get('token')
    
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
            update_query = ", ".join([f"{field} = %s" for field in update_data.keys()])
            values = list(update_data.values())
            values.append(token)
            app.logger.debug(update_query)
            app.logger.debug(values)

            cursor.execute(f"UPDATE users SET {update_query} WHERE token = %s;", values)
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


if __name__ == '__main__':
    logger = logging.getLogger("server")
    logger.setLevel(logging.DEBUG)
    app.run(debug=True, port=5001, host='0.0.0.0')
