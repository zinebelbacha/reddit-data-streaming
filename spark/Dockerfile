FROM bitnami/spark:3.5.0


WORKDIR /opt/bitnami/spark

RUN pip install py4j psycopg2-binary

COPY ./src/spark_pg/spark_streaming.py ./spark_streaming.py 


ENV POSTGRES_DOCKER_USER=host.docker.internal
ARG PG_PASSWORD
ENV PG_PASSWORD=$PG_PASSWORD