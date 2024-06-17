#!/bin/sh

psql --variable=ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE "$POSTGRES_DB";
    GRANT ALL PRIVILEGES ON DATABASE "$POSTGRES_DB" TO "$POSTGRES_USER";
    CREATE EXTENSION IF NOT EXISTS pgcrypto;
EOSQL

psql --variable=ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE tasks (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        content TEXT,
        date_of_creation TEXT,
        deadline TEXT,
        status INT
    );
EOSQL
