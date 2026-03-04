from checks import (
    get_daily_counts,
    get_null_rates,
    get_missing_dates,
    compare_stg_vs_fact,
)

import ollama


def detect_anomalies(counts_df, nulls_df, missing_df, compare_df):
    anomalies = []

    # Missing dates
    if len(missing_df) > 0:
        anomalies.append(
            f"Missing partitions/dates detected: {missing_df['missing_date'].tolist()}"
        )

    # Row count drop detection (simple)
    if len(counts_df) >= 2:
        counts_df = counts_df.copy()
        counts_df["prev"] = counts_df["row_count"].shift(1)
        counts_df["pct_change"] = (
            (counts_df["row_count"] - counts_df["prev"]) / counts_df["prev"]
        ) * 100
        bad = counts_df[counts_df["pct_change"] < -40]
        if len(bad) > 0:
            for _, r in bad.iterrows():
                anomalies.append(f"Row count drop on {r['order_date']}: {r['pct_change']:.1f}%")

    # Null spike
    bad_nulls = nulls_df[nulls_df["null_pct"] > 10]
    if len(bad_nulls) > 0:
        for _, r in bad_nulls.iterrows():
            anomalies.append(f"Null spike on {r['order_date']}: amount null_pct={r['null_pct']}%")

    # Revenue mismatch
    bad_rev = compare_df[compare_df["revenue_diff"].abs() > 0.01]
    if len(bad_rev) > 0:
        big = bad_rev[bad_rev["revenue_diff"].abs() > 100]
        if len(big) > 0:
            for _, r in big.iterrows():
                anomalies.append(
                    f"Revenue mismatch on {r['order_date']}: "
                    f"stg={r['stg_revenue']} vs fct={r['fct_revenue']} (diff={r['revenue_diff']})"
                )

    return anomalies


def build_report(mode="healthy", model="llama3.1:8b"):
    counts = get_daily_counts("stg_orders", 7)
    nulls = get_null_rates("stg_orders", 7)
    missing = get_missing_dates("stg_orders", 7)
    compare = compare_stg_vs_fact(7)

    anomalies = detect_anomalies(counts, nulls, missing, compare)

    prompt = f"""
You are a senior data engineer acting as a Data Quality Agent.

You are given check results from a data pipeline with layers:
raw_orders -> stg_orders -> fct_sales

Write a clear, practical report for a data team.

Rules:
- Be concise and structured.
- If anomalies exist, explain likely root causes and propose next investigation steps.
- Include SQL validation ideas.
- Do NOT claim you executed fixes.
- If no anomalies, say the pipeline looks healthy.

Failure mode used for test data: {mode}

ANOMALIES DETECTED:
{anomalies}

COUNTS DF:
{counts.to_string(index=False)}

NULL RATES DF:
{nulls.to_string(index=False)}

MISSING DATES DF:
{missing.to_string(index=False)}

STG VS FACT DF:
{compare.to_string(index=False)}
"""

    response = ollama.chat(
        model=model,
        messages=[
            {"role": "system", "content": "You are a careful data quality investigator."},
            {"role": "user", "content": prompt},
        ],
    )

    return {
        "anomalies": anomalies,
        "counts": counts,
        "nulls": nulls,
        "missing": missing,
        "compare": compare,
        "report": response["message"]["content"],
    }
