import os
import sqlite3
from flask import Flask, request, jsonify

import util

app = Flask(__name__)
db_path = '../data/users.db' # По факту обычный файлик, если он будет лежать в отдельном контейнере, то 1) как к нему получить доступ из другого контейнера? 2) Как сделаь так, чтобы контейнер с бд работал бесконечно(с костылями поянтно как)


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
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM users WHERE username=?", (username,))
    user = cursor.fetchone()

    if user:
        conn.close()
        return jsonify({'message': 'User already exist'}), 400
    else:
        cursor.execute("INSERT INTO users (username, password) VALUES (?, ?)",
                   (username, hashed_password))
        conn.commit()
        conn.close()
        return jsonify({'message': 'User successfully registered'})

@app.route('/update', methods=['PUT'])
def update_user():
    data = request.json
    token = data.get('token')
    
    if not token:
        return jsonify({'message': 'Token is missing'}), 400

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM users WHERE token=?", (token,))
    user = cursor.fetchone()

    if not user:
        conn.close()
        return jsonify({'message': 'Invalid token'}), 401

    update_data = {}
    fields = ['first_name', 'last_name', 'date_of_birth', 'email', 'phone_number']
    for field in fields:
        if data.get(field):
            update_data[field] = data[field]

    update_query = ", ".join([f"{field} = ?" for field in update_data.keys()])
    values = list(update_data.values())
    values.append(token)

    cursor.execute(f"UPDATE users SET {update_query} WHERE token=?", values)
    conn.commit()
    conn.close()

    return jsonify({'message': 'User information successfully updated'})

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
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    hashed_password = util.hash_password(password)

    cursor.execute("SELECT * FROM users WHERE username=? AND password=?", (username, hashed_password))
    user = cursor.fetchone()

    if user:
        token = util.generate_token()
        cursor.execute("UPDATE users SET token=? WHERE username=?", (token, username))
        conn.commit()
        conn.close()
        return jsonify({'message': 'User authenticated successfully', 'token': token})
    else:
        conn.close()
        return jsonify({'message': 'User does not exist or password is wrong'}), 401


if __name__ == '__main__':
    print(db_path)
    app.run(debug=True, port=5001, host='0.0.0.0')
