FROM bitnami/spark:3.5.0

WORKDIR /opt/bitnami/spark

RUN pip install pyspark py4j psycopg2-binary python-dotenv

COPY ./src/spark_pg/spark_streaming.py ./spark_streaming.py 
COPY ./configs ./configs

#ENV POSTGRES_DOCKER_USER=host.docker.internal