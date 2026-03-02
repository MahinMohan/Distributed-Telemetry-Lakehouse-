# Trino (Lakehouse Query Layer)

This folder contains Trino configs to query telemetry data in an S3-backed lakehouse using the Hive connector.

## What’s included
- Trino coordinator config
- JVM config
- Node config
- Hive catalog for S3 + Glue (or optional Hive Metastore)

## Quick start (docker)
You can run Trino via docker-compose from the repo root (recommended), or directly with docker.

Trino UI: http://localhost:8080
