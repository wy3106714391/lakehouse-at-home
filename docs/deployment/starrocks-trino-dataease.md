# StarRocks, Trino, and DataEase Integration Guide

## Overview

This guide explains how to use the newly added components in the lakehouse platform:

- **StarRocks**: High-performance OLAP database for sub-second queries
- **Trino**: Distributed SQL query engine for federated queries across data sources
- **DataEase**: Data visualization and BI platform

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  DataEase   │────▶│    Trino    │────▶│  StarRocks  │
│  (BI/Vis)   │     │(Query Layer)│     │ (OLAP DB)   │
└─────────────┘     └──────┬──────┘     └──────┬──────┘
                           │                   │
                           ▼                   ▼
                    ┌─────────────┐     ┌─────────────┐
                    │   Polaris   │────▶│  SeaweedFS  │
                    │   Catalog   │     │  (S3 Store) │
                    └─────────────┘     └─────────────┘
```

## Component Details

### StarRocks

**Ports:**
- `8030`: FE HTTP port (web UI)
- `9020`: FE RPC port
- `9030`: MySQL protocol port (connect via MySQL clients)
- `8040`: BE web server port
- `9050`: BE RPC port
- `9060`: BE heartbeat port

**Access:**
```bash
# Connect via MySQL client
mysql -h localhost -P 9030 -u root

# Or use any MySQL-compatible tool (DBeaver, DataGrip, etc.)
```

**Initial Setup:**
```sql
-- Create database for lakehouse analytics
CREATE DATABASE IF NOT EXISTS lakehouse_analytics;

-- Create table from Iceberg (via external catalog)
-- Note: StarRocks can read from Iceberg tables via external catalogs
```

### Trino

**Ports:**
- `8088`: Trino Web UI and REST API

**Access:**
- Web UI: http://localhost:8088
- CLI: Use the Trino CLI or any JDBC/ODBC client

**Pre-configured Catalogs:**
- `lakehouse`: Apache Iceberg catalog connected to Polaris

**Example Queries:**
```sql
-- List all schemas in the lakehouse catalog
SHOW SCHEMAS FROM lakehouse;

-- Query Iceberg tables
SELECT * FROM lakehouse.bronze.raw_data LIMIT 100;

-- Join across different data sources
SELECT 
    t1.customer_id,
    t1.total_amount,
    t2.customer_name
FROM lakehouse.silver.orders t1
JOIN starrocks.lakehouse_analytics.customers t2
    ON t1.customer_id = t2.id;
```

### DataEase

**Ports:**
- `8081`: Web UI

**Access:**
- URL: http://localhost:8081
- Default credentials: Check DataEase documentation (typically admin/DataEase@123456)

**Connecting to Data Sources:**

1. **Connect to StarRocks:**
   - Type: MySQL
   - Host: starrocks-fe
   - Port: 9030
   - Database: lakehouse_analytics
   - Username: root
   - Password: (leave empty or set via environment)

2. **Connect to Trino:**
   - Type: Trino/Presto
   - Host: trino-coordinator
   - Port: 8080
   - Catalog: lakehouse

3. **Connect to PostgreSQL:**
   - Type: PostgreSQL
   - Host: postgres
   - Port: 5432
   - Database: mage_data / polaris_catalog / openmetadata_db

## Usage Patterns

### Pattern 1: Fast Analytics with StarRocks

1. Ingest data into Iceberg tables via Mage.ai pipelines
2. Create StarRocks external tables or import data
3. Build dashboards in DataEase connecting to StarRocks
4. Benefit from sub-second query performance

### Pattern 2: Federated Queries with Trino

1. Query Iceberg tables directly through Trino
2. Join with data from other sources (PostgreSQL, MySQL, etc.)
3. Use Trino's pushdown optimization for efficient queries
4. Visualize results in DataEase

### Pattern 3: End-to-End Data Flow

```
Raw Data → Mage.ai → Iceberg (SeaweedFS) → StarRocks → DataEase
                                    ↓
                                Trino (ad-hoc queries)
                                    ↓
                              OpenMetadata (lineage)
```

## Configuration Files

### Trino Catalog Configuration

Location: `config/trino/coordinator/catalog/lakehouse.properties`

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://polaris-catalog:8100
iceberg.rest-catalog.warehouse=s3a://lakehouse/warehouse
hive.s3.endpoint=http://seaweedfs:8333
hive.s3.aws-access-key=admin
hive.s3.aws-secret-key=admin123
hive.s3.path-style-access=true
```

### StarRocks External Catalog (Optional)

To connect StarRocks to Iceberg, create an external catalog:

```sql
CREATE EXTERNAL CATALOG iceberg_catalog
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.rest.uri" = "http://polaris-catalog:8100",
    "aws.access.key" = "admin",
    "aws.secret.key" = "admin123",
    "aws.endpoint" = "http://seaweedfs:8333"
);
```

## Troubleshooting

### StarRocks Issues

1. **FE not starting:**
   ```bash
   docker logs starrocks-fe
   # Check meta directory permissions
   ```

2. **BE not connecting to FE:**
   ```bash
   docker logs starrocks-be
   # Ensure FE is healthy before BE starts
   ```

### Trino Issues

1. **Catalog not loading:**
   ```bash
   docker logs trino-coordinator
   # Check catalog configuration files
   ```

2. **Cannot connect to Iceberg:**
   - Verify Polaris Catalog is running
   - Check S3 endpoint accessibility
   - Validate credentials

### DataEase Issues

1. **Cannot connect to databases:**
   - Ensure all services are on the same Docker network
   - Check service health with `docker compose ps`
   - Verify connection parameters

## Best Practices

1. **Resource Allocation:**
   - StarRocks BE: Allocate sufficient memory (8GB+ recommended)
   - Trino Worker: Adjust JVM heap based on query complexity
   - DataEase: Requires MySQL and MinIO dependencies

2. **Performance Tuning:**
   - Use StarRocks for high-frequency analytical queries
   - Use Trino for ad-hoc federated queries
   - Materialize frequently accessed data in StarRocks

3. **Security:**
   - Change default passwords in production
   - Enable SSL/TLS for all connections
   - Configure proper network isolation

## Next Steps

1. Start the full stack:
   ```bash
   docker compose -f docker-compose-full.yml up -d
   ```

2. Verify all services:
   ```bash
   docker compose -f docker-compose-full.yml ps
   ```

3. Access individual services:
   - StarRocks: mysql -h localhost -P 9030
   - Trino: http://localhost:8088
   - DataEase: http://localhost:8081

4. Create your first pipeline in Mage.ai to populate Iceberg tables

5. Build visualizations in DataEase using the query layer
