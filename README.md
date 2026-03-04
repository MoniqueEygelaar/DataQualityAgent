# Data Quality Agent (MVP)
Exploring Agentic AI in Data Engineering

## Overview

This project is a learning-focused MVP built to explore how agentic AI can support data quality workflows in analytics engineering.

It is intentionally simple and experimental. The goal is to understand:

- How LLMs can use tools such as SQL execution
- How agents can reason over pipeline states
- How investigation workflows can be structured safely
- What guardrails are required in data systems

This is not production-ready software. It is a sandbox for exploration and hands-on learning.

---

## Project Motivation

Modern data teams spend significant time investigating:

- Missing partitions  
- Row count drops  
- Null spikes  
- Revenue mismatches  
- Inconsistencies between pipeline layers  

This MVP explores a lightweight approach where an AI-driven agent:

1. Runs deterministic data quality checks  
2. Detects anomalies  
3. Gathers evidence  
4. Generates a structured investigation report  
5. Suggests next validation steps (without auto-executing fixes)

The aim is to simulate how a data engineer would investigate issues, while keeping the system safe and read-only.

---

## Architecture
```
raw_orders
↓
stg_orders
↓
fct_sales
```

### Stack

- DuckDB — Local analytical database  
- Pandas / NumPy — Data simulation  
- Streamlit — UI  
- Ollama (Llama 3) — Local LLM for report generation  
- Python — Agent orchestration  

The system runs fully locally. No external APIs are required.

---

## What the Agent Checks

For the last 7 days:

- Row count changes (detects significant drops)  
- Null spikes in critical columns  
- Missing date partitions  
- Revenue mismatches between staging and fact tables  

If anomalies are found, the agent:

- Explains likely root causes  
- Suggests validation SQL  
- Proposes investigation steps  

No fixes are executed automatically.

---

## Failure Scenarios

The app allows simulation of common pipeline failures:

- `healthy`  
- `missing_day`  
- `null_spike`  
- `revenue_mismatch`  
- `row_drop`  

These scenarios make it easier to test investigation behavior and reasoning.

---

## Setup

Clone the repository:

```bash
git clone https://github.com/yourusername/dq-agent-mvp.git
cd dq-agent-mvp
```

Create a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```
Install dependencies:
```bash
pip install duckdb pandas numpy streamlit ollama
```
Install Ollama and pull a model:
```bash
ollama pull llama3.1:8b
```
Run the app:
```bash
streamlit run app.py
```
