from dotenv import load_dotenv
import os

load_dotenv()
##########################################################
#################   MSSQL SERVER #########################
##########################################################

DB_SQL_DATABASE = os.getenv("DB_SQL_DATABASE")
DB_SQL_USER = os.getenv("DB_SQL_USER")
DB_SQL_PASSWORD = os.getenv("DB_SQL_PASSWORD")
DB_SQL_DRIVER = os.getenv("DB_SQL_DRIVER")

# print(DB_SQL_DATABASE, DB_SQL_USER)   # შესამოწმებლად
connection_properties = {
    "user": DB_SQL_USER,
    "password": DB_SQL_PASSWORD,  # your real sa password
    "driver": DB_SQL_DRIVER
}


jdbc_url = f"jdbc:sqlserver://localhost:1433;database={DB_SQL_DATABASE};encrypt=false;"



##########################################################
#################   POSTGRESQL SERVER #########################
##########################################################



DB_POSTGRESQL_DATABASE = os.getenv("DB_POSTGRESQL_DATABASE")
DB_POSTGRESQL_USER = os.getenv("DB_POSTGRESQL_USER")
DB_POSTGRESQL_PASSWORD = os.getenv("DB_POSTGRESQL_PASSWORD")
DB_POSTGRESQL_DRIVER = os.getenv("DB_POSTGRESQL_DRIVER")
DB_POSTGRESQL_HOST = os.getenv("DB_POSTGRESQL_HOST", "localhost")
DB_POSTGRESQL_PORT = os.getenv("DB_POSTGRESQL_PORT", "5432")


# Spark JDBC
POSTGRES_URL = f"jdbc:postgresql://{DB_POSTGRESQL_HOST}:{DB_POSTGRESQL_PORT}/{DB_POSTGRESQL_DATABASE}"
POSTGRES_PROPERTIES = {
    "user": DB_POSTGRESQL_USER,
    "password": DB_POSTGRESQL_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# psycopg2 (Silver layer procedures)
POSTGRES_PG_CONFIG = {
    "host": DB_POSTGRESQL_HOST,
    "port": DB_POSTGRESQL_PORT,
    "dbname": DB_POSTGRESQL_DATABASE,
    "user": DB_POSTGRESQL_USER,
    "password": DB_POSTGRESQL_PASSWORD
}


