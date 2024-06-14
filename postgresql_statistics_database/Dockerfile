FROM postgres:14

COPY ./data /var/lib/postgresql/data
COPY ./docker-entrypoint.sh /docker-entrypoint-initdb.d/initdb.sh

RUN chmod 777 /docker-entrypoint-initdb.d/initdb.sh