# Adaptive Hybrid Database Ingestion Framework
**CS432 – Databases Course Project**  

---

## Overview
This project implements a **self-adaptive data ingestion framework** that automatically decides whether incoming JSON fields should be stored in a **relational database (SQL / SQLite)** or a **document database (MongoDB)**.

The system consumes a heterogeneous live data stream, analyzes field behavior, infers schema characteristics, and routes each attribute to the most suitable storage backend while preserving a unified logical record.

---

## Key Concept
| Data Behavior | Storage Backend |
|-------------|-------------|
Stable, frequent, scalar | SQL |
Unstable, nested, evolving | MongoDB |

Decisions are learned automatically using observed statistics — no predefined schema or manual mapping.

---

## Logical Join Keys
Every record stores the following fields in **both databases**:

- `username` → logical entity identifier  
- `sys_ingested_at` → ingestion timestamp  

These allow reconstruction of a single logical record across heterogeneous storage systems.

---

## Project Structure

| File | Purpose |
|------|------|
`simulation_code.py` | Generates live heterogeneous JSON stream |
`main.py` | Main adaptive ingestion pipeline |
`normalizer.py` | Field normalization & flattening |
`analyzer.py` | Tracks frequency, type stability & uniqueness |
`heuristics.py` | Decides SQL vs MongoDB placement |
`metadata_store.py` | Persists learned schema decisions |
`sql_backend.py` | Inserts structured data into SQLite |
`mongo_backend.py` | Inserts flexible data into MongoDB |
`metadata.json` | Stored schema memory (auto-generated) |
`logs.json `| Storing logs| 
---


