from tools import run_sql

def get_daily_counts(table: str, days: int = 7):
    return run_sql(f"""
        SELECT
            order_date,
            COUNT(*) AS row_count
        FROM {table}
        WHERE order_date >= CURRENT_DATE - INTERVAL '{days} days'
        GROUP BY 1
        ORDER BY 1;
    """)

def get_null_rates(table: str, days: int = 7):
    return run_sql(f"""
        SELECT
            order_date,
            COUNT(*) AS rows,
            SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amounts,
            ROUND(100.0 * SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_pct
        FROM {table}
        WHERE order_date >= CURRENT_DATE - INTERVAL '{days} days'
        GROUP BY 1
        ORDER BY 1;
    """)

def get_missing_dates(table: str, days: int = 7):
    return run_sql(f"""
        WITH expected AS (
            SELECT (CURRENT_DATE - CAST(i AS INTEGER)) AS d
            FROM range(0, {days + 1}) t(i)
        ),
        actual AS (
            SELECT DISTINCT order_date AS d
            FROM {table}
            WHERE order_date >= CURRENT_DATE - INTERVAL '{days} days'
        )
        SELECT expected.d AS missing_date
        FROM expected
        LEFT JOIN actual USING(d)
        WHERE actual.d IS NULL
        ORDER BY 1;
    """)


def compare_stg_vs_fact(days: int = 7):
    return run_sql(f"""
        WITH stg AS (
            SELECT
                order_date,
                COUNT(*) AS stg_orders,
                SUM(amount) AS stg_revenue
            FROM stg_orders
            WHERE order_date >= CURRENT_DATE - INTERVAL '{days} days'
            GROUP BY 1
        ),
        fct AS (
            SELECT
                order_date,
                orders AS fct_orders,
                revenue AS fct_revenue
            FROM fct_sales
            WHERE order_date >= CURRENT_DATE - INTERVAL '{days} days'
        )
        SELECT
            COALESCE(stg.order_date, fct.order_date) AS order_date,
            stg_orders,
            fct_orders,
            ROUND(stg_revenue, 2) AS stg_revenue,
            ROUND(fct_revenue, 2) AS fct_revenue,
            ROUND(fct_revenue - stg_revenue, 2) AS revenue_diff
        FROM stg
        FULL OUTER JOIN fct USING(order_date)
        ORDER BY 1;
    """)
