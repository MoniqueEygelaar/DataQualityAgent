import duckdb

DB_PATH = "dq_agent.duckdb"

def run_sql(query: str):
    con = duckdb.connect(DB_PATH)
    try:
        df = con.execute(query).df()
        return df
    finally:
        con.close()

def get_schema(table_name: str):
    q = f"DESCRIBE {table_name};"
    return run_sql(q)
