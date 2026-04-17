# HybridDB: Adaptive Ingestion & Unified Query Framework (SADF)

HybridDB is a high-performance database abstraction layer that transparently bridges **relational (SQLite)** and **document-based (MongoDB)** storage. It uses an adaptive routing engine to store data where it fits best while providing a single, unified JSON-based logical query interface to the user.

---

## 🚀 Key Features

- **Adaptive Data Routing**: Automatically routes fields to SQL or NoSQL backends based on data characteristics.
- **Unified Query Engine**: Query across heterogenous backends using a single JSON syntax.
- **Temporal Merging**: Seamlessly joins data from multiple backends using the `sys_ingested_at` temporal key.
- **Performance Tracing**: Real-time auditing of execution times across different storage layers.
- **ACID-Aware Transactions**: Distributed transaction management with manual compensation for hybrid consistency.

---

## 🛠️ Prerequisites

Before you begin, ensure you have the following installed:
- **Python 3.9+**
- **MongoDB** (Running locally on `mongodb://localhost:27017/`)
- **SQLite3** (Usually bundled with Python)

---

## 📦 Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-username/hybrid-db-sadf.git
   cd hybrid-db-sadf
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

---

## ⚙️ Backend Configuration

### 1. SQLite (SQL Backend)
The system automatically creates a `hybrid.db` file in the root directory upon the first run. No manual schema creation is required; the engine handles **Schema Evolution** automatically by altering tables as new fields arrive.

### 2. MongoDB (NoSQL Backend)
Ensure your MongoDB service is running. By default, the system connects to:
- **URI**: `mongodb://localhost:27017/`
- **Database**: `hybrid_db`
- **Root Collection**: `users`

To change these settings, modify the constants in `mongo_backend.py`.

---

## 🏃 Running the System

The system consists of two primary components that should be run in separate terminals:

### 1. Ingestion Engine
The ingestion engine connects to an external SSE record stream, analyzes incoming data, updates metadata, and routes records to the appropriate backends.
```bash
python main.py
```

### 2. Dashboard & Query API
The Flask-based dashboard provides a visual interface for exploring data and a REST API for executing logical queries.
```bash
python app.py
```
Access the dashboard at: **`http://localhost:5000/`**

---

## 🔍 Logical Query Interface

You can interact with the system via the **SADF Query Interface** or directly via the REST API.

### Unified Query Example
**Endpoint**: `POST /api/query`
**Payload**:
```json
{
  "operation": "read",
  "entity": "health_metrics",
  "fields": ["username", "spo2", "heart_rate", "city"],
  "conditions": [
    { "field": "spo2", "op": "lt", "value": 90 }
  ],
  "__trace": true
}
```

### Response (with Performance Tracing)
```json
{
  "results": [...],
  "trace": {
    "route_ms": 1.2,
    "sql_ms": 4.5,
    "mongo_ms": 8.2,
    "merge_ms": 2.1,
    "total_ms": 16.0
  }
}
```

---

## 📚 Documentation
For deep technical details on the architecture, temporal merging, and ACID validation, visit the built-in documentation page:
**`http://localhost:5000/docs`**

---

## 📂 Project Structure
- `app.py`: Flask dashboard and REST API entry point.
- `main.py`: Ingestion logic and stream listener.
- `query_engine.py`: Core logic for routing, merging, and tracing.
- `sql_backend.py` / `mongo_backend.py`: Backend-specific drivers.
- `metadata.json`: The source of truth for field-to-backend routing.
- `templates/`: UI components for the dashboard and documentation.

---
**Architected for Advanced Agentic Coding Assignments**
