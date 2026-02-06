import psycopg2
from Utils.config import POSTGRES_PG_CONFIG

def call_postgres_procedure(proc_name, is_function=False):
    conn = psycopg2.connect(**POSTGRES_PG_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    if is_function:
        cur.execute(f"SELECT {proc_name}();")
    else:
        cur.execute(f"CALL {proc_name}();")

    cur.close()
    conn.close()
