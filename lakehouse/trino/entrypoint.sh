set -euo pipefail

mkdir -p /etc/trino/catalog

cat > /etc/trino/node.properties <<'EOF'
node.environment=telemetry
node.id=trino-coordinator-1
node.data-dir=/var/trino/data
EOF

cat > /etc/trino/jvm.config <<'EOF'
-server
-Xmx4G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
EOF

cat > /etc/trino/config.properties <<'EOF'
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080

query.max-memory=2GB
query.max-memory-per-node=1GB
query.max-total-memory-per-node=2GB

discovery-server.enabled=true
discovery.uri=http://localhost:8080
EOF

cat > /etc/trino/log.properties <<'EOF'
io.trino=INFO
EOF

cat > /etc/trino/catalog/hive.properties <<'EOF'
connector.name=hive
hive.metastore=glue
hive.s3.region=${ENV:AWS_REGION}
hive.s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}
hive.s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}

hive.s3.path-style-access=true
hive.non-managed-table-writes-enabled=true
hive.storage-format=PARQUET
EOF

cat > /etc/trino/catalog/system.properties <<'EOF'
connector.name=system
EOF

exec /usr/lib/trino/bin/run-trino