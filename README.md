# Telemetry Lakehouse Streaming System

A real time telemetry processing platform designed for low latency ingestion, scalable stream processing, and interactive analytics.

The system ingests live or simulated telemetry events, processes them using Apache Flink, stores datasets in an AWS S3 lakehouse, and exposes insights through Trino and a Streamlit dashboard.

---

## System Architecture

### C++ Ingestion Engine
- Ingests live or simulated telemetry data
- Serializes events into JSON payloads
- Publishes messages to Kafka topics
- Designed for low latency and high throughput

### Kafka Message Broker
- Serves as the central event backbone
- Provides durable, ordered, and fault tolerant streams
- Decouples ingestion, processing, and storage layers

Topics:
- telemetry.raw
- telemetry.clean
- telemetry.anomalies

### Apache Flink Stream Processor
- Consumes telemetry from Kafka
- Performs parsing, normalization, and enrichment
- Computes sliding window metrics
- Detects anomalous patterns using configurable thresholds
- Writes outputs to Kafka and AWS S3

### AWS S3 Lakehouse
- Stores raw, cleaned, and derived telemetry datasets
- Provides scalable low cost long term storage
- Supports downstream analytical workloads

Storage layout:
telemetry-lake/
├── raw/
├── clean/
└── anomalies/


S3 paths:
s3://telemetry-lake/raw/
s3://telemetry-lake/clean/
s3://telemetry-lake/anomalies/


Formats:
- JSON
- CSV
- Parquet

### Trino SQL Engine
- Executes interactive SQL queries directly on S3 data
- Enables ad hoc analysis and exploratory analytics
- Supports ANSI SQL for BI and reporting tools

Example analytics:
- Metric trends over time
- Anomaly frequency analysis
- Aggregation and distribution queries
- Historical performance analysis

### Streamlit Dashboard
- Visualizes telemetry metrics and system health
- Displays near real time summaries
- Supports validation and inspection of streaming pipelines

---

## Repository Structure
telemetry_lakehouse/
│── README.md
│── ingestion/
│ └── cpp/
│ └── main.cpp
│── streaming/
│ └── flink_job.py
│── dashboard/
│ └── streamlit_app.py
│── lakehouse/
│ └── trino/
│ └── schema.sql
└── infra/
└── docker/
└── Dockerfile


---

## Component Details

### C++ Ingestion Engine
Implemented in C++20 and optimized for low latency streaming.

Technologies:
- nlohmann/json for serialization
- librdkafka or cppkafka for Kafka publishing
- libcurl or cpr for external data ingestion

### Kafka Streaming Layer
Kafka provides reliable event delivery between ingestion and processing layers, enabling horizontal scalability and fault isolation.

### Apache Flink Processing Pipeline
The Flink job performs:
- JSON parsing and schema normalization
- Rolling window aggregations
- Metric computation
- Anomaly detection
- Output routing to Kafka and S3

The pipeline operates with sub second end to end latency under sustained throughput.

### Lakehouse Storage (AWS S3)
Data is organized by processing stage:
- Raw telemetry
- Clean telemetry
- Derived anomaly datasets

This structure supports efficient analytics and long term retention.

### Trino Query Engine
Trino enables interactive analytics directly on lakehouse data without data movement.

Typical use cases:
- Time series analysis
- Anomaly trend monitoring
- Aggregation queries
- Historical system performance analysis

### Streamlit Dashboard
Provides:
- Telemetry visualizations
- System health indicators
- Near real time summaries
- Streaming pipeline validation views

---

## Running Locally

### Streamlit Dashboard
pip install streamlit
streamlit run dashboard/streamlit_app.py
