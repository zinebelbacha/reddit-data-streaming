import os

#URL = "jdbc:postgresql://host.docker.internal:5432/postgres"
# URL = "jdbc:postgresql://postgres:5432/postgres"
# POSTGRES_PROPERTIES = {
#     "user": "postgres",
#     "password": os.getenv("PG_PASSWORD"),
#     "driver": "org.postgresql.Driver"
# }
DB_NAME = "postgres"
DB_USER = "postgres"
PORT = 5432
#DB_HOST = "localhost"   # for table creation
DB_HOST = "host.docker.internal"
URL = f"jdbc:postgresql://{DB_HOST}:5432/postgres"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": os.getenv("PG_PASSWORD"),
    "driver": "org.postgresql.Driver"
}
