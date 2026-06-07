# 🌐 Wikipedia Distributed Search Engine
### Parallel and Distributed Computing (PDC) Final Project 2026

A professional-grade, distributed full-text search engine powered by **Apache Spark**, **Elasticsearch**, and **Apache Solr**. This system processes 572k+ Wikipedia articles and provides a side-by-side analytics dashboard for search comparison.

---

## 🏗️ System Architecture
The project implements a high-performance **Data Ingestion Pipeline**:
1.  **Distributed Parsing:** Raw Wikipedia XML (2GB+) is parsed using **PySpark** on a multi-core architecture.
2.  **Streaming Data Flow:** Data is transformed into **JSONL** to maintain a constant memory footprint ($O(1)$ complexity).
3.  **Parallel Indexing:** Articles are indexed simultaneously into **Elasticsearch** (v8.x) and **Apache Solr** (v9.x) using high-speed streaming bulk APIs.
4.  **Full-Stack Dashboard:** A **Flask** backend orchestrates parallel queries to both engines, returning unified analytics to a modern Bootstrap 5 UI.

---

## 📂 Final Project Structure
```text
FinalExam/
├── data/                      # Wikipedia Datasets (XML/JSONL)
├── spark/
│   └── parse_wikipedia.py     # Distributed XML Parser (PySpark)
├── elasticsearch/
│   └── index_elasticsearch.py # Streaming Bulk Indexer (Elasticsearch)
├── solr/
│   └── index_solr.py          # Batch Indexer (Apache Solr)
├── frontend/
│   ├── app.py                 # Flask API Gateway
│   └── templates/
│       └── index.html         # Analytics Dashboard (UI/UX)
├── scripts/
│   ├── run_all.py             # Pipeline Automation Script
│   └── download_dataset.py    # Wikipedia Data Downloader
├── docker-compose.yml         # Container Orchestration (ES, Solr, Kibana)
├── VIVA_QA.md                 # Preparation for Examination
└── README.md                  # Project Documentation
```

---

## 🚀 Key Features
-   **Dual-Engine Comparison:** Live benchmarks comparing Elasticsearch vs Solr response times.
-   **Advanced Cleaning:** Custom regex engine strips raw Wikipedia markup for a clean reading experience.
-   **In-Page Article Modal:** Read full articles directly within the dashboard using an optimized modal system.
-   **Real-Time Analytics:** Integrated **Kibana Dashboards** for monitoring index health and data distribution.
-   **Print-Ready Reports:** Custom CSS allows users to print professional search results and performance charts.

---

## 🧠 PDC Concepts Implemented
-   **Data Parallelism:** Spark RDDs distribute XML parsing across all available CPU cores.
-   **Distributed Indexing:** Sharding and replication settings configured for Elasticsearch/Solr clusters.
-   **Asynchronous Processing:** Background "Update-by-Query" tasks for schema migration without downtime.
-   **I/O Optimization:** Switched from monolithic JSON lists to Line-delimited streaming to prevent memory overflows.

---

## 🛠️ Deployment Instructions
1.  **Launch Infrastructure:**
    ```powershell
    docker-compose up -d
    ```
2.  **Run Pipeline:** (Parses data and indexes into both engines)
    ```powershell
    python scripts/run_all.py
    ```
3.  **Start Search Dashboard:**
    ```powershell
    python frontend/app.py
    ```
    Access at: `http://localhost:5000`

---
**Prepared by Sufyan Ahmad | PDC Final Project 2026**
