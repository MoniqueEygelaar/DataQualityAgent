import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

DB_PATH = "dq_agent.duckdb"

def _date_range(days=14):
    today = datetime.utcnow().date()
    return [today - timedelta(days=i) for i in range(days)][::-1]

def create_base_data(days=14):
    rows = []
    for d in _date_range(days):
        n = random.randint(80, 130)
        for i in range(n):
            order_id = f"{d.strftime('%Y%m%d')}-{i:04d}"
            amount = round(max(0, np.random.normal(120, 35)), 2)
            customer_id = random.randint(1000, 1200)
            rows.append(
                {
                    "order_id": order_id,
                    "order_date": str(d),
                    "customer_id": customer_id,
                    "amount": amount,
                }
            )
    return pd.DataFrame(rows)

def apply_failure(df_raw, mode: str):
    """
    Modes:
      - healthy
      - missing_day
      - null_spike
      - revenue_mismatch
      - row_drop
    """
    df = df_raw.copy()

    if mode == "missing_day":
        # Remove all rows for 2 days ago
        target = (datetime.utcnow().date() - timedelta(days=2)).isoformat()
        df = df[df["order_date"] != target]

    elif mode == "null_spike":
        # Make amounts null for 1 day ago
        target = (datetime.utcnow().date() - timedelta(days=1)).isoformat()
        mask = df["order_date"] == target
        # Null 50% of rows
        idx = df[mask].sample(frac=0.5, random_state=42).index
        df.loc[idx, "amount"] = None

    elif mode == "revenue_mismatch":
        # Later: we’ll introduce mismatch in fact layer
        pass

    elif mode == "row_drop":
        # Reduce yesterday rows by 70%
        target = (datetime.utcnow().date() - timedelta(days=1)).isoformat()
        day_df = df[df["order_date"] == target]
        keep = day_df.sample(frac=0.3, random_state=42).index
        df = pd.concat([df[df["order_date"] != target], df.loc[keep]])

    return df

def setup_db(mode="healthy"):
    con = duckdb.connect(DB_PATH)

    # Drop tables if exist
    con.execute("DROP TABLE IF EXISTS raw_orders;")
    con.execute("DROP TABLE IF EXISTS stg_orders;")
    con.execute("DROP TABLE IF EXISTS fct_sales;")

    # Create raw
    df_raw = create_base_data(days=14)
    df_raw = apply_failure(df_raw, mode)

    con.execute("CREATE TABLE raw_orders AS SELECT * FROM df_raw;")

    # Staging (simple pass-through + some type casting)
    con.execute("""
        CREATE TABLE stg_orders AS
        SELECT
            order_id,
            CAST(order_date AS DATE) AS order_date,
            customer_id,
            CAST(amount AS DOUBLE) AS amount
        FROM raw_orders;
    """)

    # Fact table (aggregate)
    con.execute("""
        CREATE TABLE fct_sales AS
        SELECT
            order_date,
            COUNT(*) AS orders,
            SUM(amount) AS revenue
        FROM stg_orders
        GROUP BY 1
        ORDER BY 1;
    """)

    # Introduce mismatch only at fact level
    if mode == "revenue_mismatch":
        # reduce revenue for yesterday
        target = (datetime.utcnow().date() - timedelta(days=1)).isoformat()
        con.execute(f"""
            UPDATE fct_sales
            SET revenue = revenue * 0.65
            WHERE order_date = DATE '{target}';
        """)

    con.close()

if __name__ == "__main__":
    setup_db("healthy")
    print("Database created:", DB_PATH)
