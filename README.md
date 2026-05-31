# 📈 StockIQ – Indian Market Intelligence Platform

StockIQ is a high-performance, asynchronous market intelligence and analytics engine designed for the Indian equity markets (NSE/BSE). Built using Python 3.11+ and FastAPI, the platform processes real-time ticketing feeds, runs complex analytical screening workflows, and broadcasts streaming updates to consumers via WebSockets.

---

## 🚀 Core Architecture Features

* **Asynchronous Lifespan Management:** Managed database connection pools (PostgreSQL/TimescaleDB) and Redis layers without blocking the event loop.
* **Dual Ingestion Pipelines:** Background `asyncio` tasks concurrently manage streaming market tick data (Kite Connect API) and live news feeds with integrated sentiment engine hooks.
* **WebSocket Streaming:** Leverages Redis Pub/Sub backends to orchestrate multi-client, low-latency live price broadcasting.
* **Data Layer Optimization:** Implements time-series optimized schema structures via TimescaleDB alongside Redis data structures for sub-millisecond lookups.

---

## 📂 File-by-File Technical Directory Breakdown

Here is how the modules map directly into this architecture:

### 🎮 Entry Point & Routing
* **`main.py`** – The core conductor of the app. It initializes FastAPI, attaches performance middlewares (GZip compression and CORS rules), registers specific API/WebSocket routers, and handles the asynchronous safe startup/shutdown cycle of backend services.
* **`config.py`** – Centralized application configurations managed using environmental variables (e.g., database connection strings, API secrets, allowed CORS origins).

### 🔄 Ingestion & Streaming Layer
* **`market_data_ingestion.py`** – Connects to upstream market feeds (like Zerodha Kite Connect), processes raw ticker data, and writes the high-frequency stream directly to Redis.
* **`news_ingestion.py`** – Periodically fetches live Indian financial market news feeds and passes payload elements to the internal processing pipelines.
* **`ws.py`** – Handles full-duplex WebSocket connection lifecycles, pulling streaming tick events from Redis Pub/Sub to push instant updates directly to client dashboards.

### 🧠 Analytics & Logic Engines
* **`classifcation_engine.py`** – Houses machine learning or pattern-matching models (likely processing sentiment classification for incoming text feeds from `news_ingestion.py`).
* **`indicator_engine.py`** – Computes mathematical technical indicators (e.g., RSI, MACD, Bollinger Bands) dynamically on high-frequency market series data.
* **`analytics.py` & `analytics.R`** – A polyglot analytical bridge. Handles structural statistical quantitative analytics—utilizing R's powerful statistical libraries alongside Python to run portfolio optimization models or historical backtests.
* **`screener.py`** – Executes high-throughput filter workflows across thousands of instruments based on technical indicators and classification logic.

### 🌐 Endpoints & Entities
* **`market.py` & `news.py`** – FastAPI endpoints serving standardized REST API routes for historical market analytics, current system status, and categorized news elements.
* **`schemas.py`** – Pydantic validation models that enforce payload constraints, serialization, and explicit typing across data endpoints.
* **`schema.sql`** – The database blueprint establishing relational models and hyper-tables (TimescaleDB) optimized for massive financial time-series logging.
