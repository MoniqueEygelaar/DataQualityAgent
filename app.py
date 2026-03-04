import streamlit as st
from db_setup import setup_db
from dq_agent import build_report

st.set_page_config(page_title="DQ Agent MVP", 
                   layout="wide")

st.title("Data Quality Agent")

mode = st.selectbox(
    "Choose a scenario",
    ["healthy", "missing_day", "null_spike", "revenue_mismatch", "row_drop"]
)

if st.button("Reset DB with selected scenario"):
    setup_db(mode)
    st.success(f"Database reset with scenario: {mode}")

if st.button("Run Agent Checks"):
    with st.spinner("Running checks + generating report..."):
        out = build_report(mode)

    st.subheader("Agent Report")
    st.write(out["report"])

    st.subheader("Detected Anomalies")
    st.write(out["anomalies"] if out["anomalies"] else ["None"])

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Row Counts")
        st.dataframe(out["counts"])
        st.subheader("Null Rates")
        st.dataframe(out["nulls"])

    with c2:
        st.subheader("Missing Dates")
        st.dataframe(out["missing"])
        st.subheader("Staging vs Fact")
        st.dataframe(out["compare"])
