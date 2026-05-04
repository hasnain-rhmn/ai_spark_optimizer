# AI Spark App Optimizer

The AI Spark App Optimizer is a diagnostic tool that leverages multi-agent artificial intelligence to automatically identify and resolve Apache Spark performance bottlenecks. 

By decoupling the data extraction process from the AI reasoning engine, this system ingests raw Spark History Server and Prometheus metrics into a structured DuckDB database, which is then analyzed by a collaborative LangGraph-based AI architecture to provide plain-English root cause analysis and actionable PySpark code fixes.

## Project Architecture

The project is split into three primary independent components:

1. **Telemetry ETL Pipeline**: Extracts data from the Spark History Server REST API and Prometheus, cleans the physical Catalyst plans, and loads the data into a local DuckDB instance.
2. **Database (DuckDB)**: A 6-table relational schema acting as the single source of truth for application execution metadata, stage durations, disk/memory spills, and hardware resource utilization.
3. **AI Optimizer Backend**: A LangGraph multi-agent system consisting of a "Planner" (Lead Spark Architect) and a "SQL Analyst" (Telemetry Data Analyst) that collaboratively query the DuckDB instance to diagnose a specific `app_id`.
