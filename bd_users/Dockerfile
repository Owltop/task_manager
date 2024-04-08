FROM python:3.9

RUN apt-get update && apt-get install -y sqlite3

WORKDIR /app
COPY . /app

CMD ["python", "create_table.py"]
