# Complete Lakehouse Platform Deployment Guide

This guide explains how to deploy the complete lakehouse platform with all components running in Docker.

## Architecture Overview

The platform includes:

| Component | Purpose | Port |
|-----------|---------|------|
| **SeaweedFS** | S3-compatible object storage | 8333 (S3), 8888 (UI) |
| **PostgreSQL** | Metadata database | 5432 |
| **Polaris Catalog** | Apache Iceberg REST Catalog | 8100 |
| **OpenMetadata** | Data governance & lineage | 8585 |
| **Apache Spark** | Distributed processing | 7077, 8080 |
| **Jupyter Lab** | Interactive development | 8888 |
| **Mage.ai** | Pipeline orchestration | 6789 |

## Quick Start

### 1. Clone and Configure

```bash
# Copy environment template
cp .env.example .env

# Review and customize settings (optional - defaults work for local dev)
vim .env
```

### 2. Start All Services

```bash
docker compose -f docker-compose-full.yml up -d
```

### 3. Verify Deployment

```bash
# Check all services are running
docker compose -f docker-compose-full.yml ps

# View logs
docker compose -f docker-compose-full.yml logs -f
```

## Accessing Services

### SeaweedFS (Object Storage)
- **S3 API**: http://localhost:8333
- **Master UI**: http://localhost:8888
- **Filer UI**: http://localhost:9333
- **Credentials**: admin / admin123

### Polaris Catalog (Iceberg REST Catalog)
- **REST API**: http://localhost:8100
- **API Docs**: http://localhost:8100/api/management/v1/catalogs
- **Catalog Name**: lakehouse

### OpenMetadata (Data Governance)
- **Web UI**: http://localhost:8585
- **API**: http://localhost:8585/api/v1
- **Default Login**: admin / admin (change in production!)

### Apache Spark
- **Master UI**: http://localhost:8080
- **Spark Master URL**: spark://localhost:7077

### Jupyter Lab
- **URL**: http://localhost:8888
- **Token**: Check container logs for access token

### Mage.ai
- **Main UI**: http://localhost:6789
- **Alternative UI**: http://localhost:6790

## Configuration Details

### PostgreSQL Databases

The PostgreSQL instance hosts multiple databases:
- `polaris_catalog` - Polaris Iceberg catalog metadata
- `openmetadata_db` - OpenMetadata configuration and lineage
- `mage_data` - Mage.ai pipeline metadata

### Storage Volumes

All data is persisted in Docker volumes:
- `seaweedfs-data` - Object storage data
- `postgres-data` - Database files
- `openmetadata-data` - OpenMetadata data
- `openmetadata-logs` - OpenMetadata logs

### Network Configuration

All services communicate via the `lakehouse-network` bridge network.
Services using `network_mode: "host"` can access host networking directly.

## Connecting to Iceberg Tables

### Spark Configuration

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("Iceberg Example")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "rest")
    .config("spark.sql.catalog.lakehouse.uri", "http://localhost:8100")
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "admin123")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:8333")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate())

# Create a table
spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.default.my_table (
    id BIGINT,
    name STRING,
    created_at TIMESTAMP
) USING iceberg
""")
```

### Python (PyIceberg)

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "lakehouse",
    **{
        "type": "rest",
        "uri": "http://localhost:8100",
        "s3.endpoint": "http://localhost:8333",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "admin123",
        "s3.path-style-access": "true",
    }
)

# List tables
tables = catalog.list_tables("default")
print(tables)
```

## Setting Up OpenMetadata Connectors

### 1. Add Iceberg Catalog Connection

1. Navigate to http://localhost:8585
2. Go to **Connectors** → **Add Connector**
3. Select **Iceberg** or **Custom** connector
4. Configure:
   - **Name**: lakehouse-polaris
   - **Catalog URI**: http://polaris-catalog:8100
   - **Warehouse**: s3a://lakehouse/warehouse
   - **S3 Endpoint**: http://seaweedfs:8333
   - **Access Key**: admin
   - **Secret Key**: admin123

### 2. Add Mage.ai Connection

1. Go to **Connectors** → **Add Connector**
2. Select **Pipeline** → **Mage**
3. Configure:
   - **Name**: mage-pipelines
   - **API URL**: http://mage-api:6789
   - **API Key**: (generate from Mage UI)

### 3. Automatic Lineage Capture

OpenMetadata will automatically capture lineage when:
- Mage.ai pipelines execute with OpenMetadata integration enabled
- Spark jobs run with OpenMetadata listener configured

Add to Spark config:
```
spark.extraListeners=com.openmetadata.server.events.SparkListener
openmetadata.api.url=http://openmetadata:8585
openmetadata.auth.token=openmetadata_api_key
```

## Mage.ai Pipeline Integration

### Configure Iceberg Connection in Mage

1. Open http://localhost:6789
2. Go to **Settings** → **Data Sources**
3. Add new connection:
   - **Type**: Custom (PyIceberg)
   - **Name**: iceberg_lakehouse
   - **Config**:
```yaml
type: rest
uri: http://localhost:8100
warehouse: s3a://lakehouse/warehouse
s3:
  endpoint: http://localhost:8333
  access_key: admin
  secret_key: admin123
  path_style: true
```

### Create Pipeline with Iceberg

See existing pipelines in `mage_projects/lakehouse_pipelines/`:
- `lakehouse_medallion/` - Multi-hop medallion architecture
- `iceberg_maintenance/` - Table maintenance tasks

## Maintenance

### Backup Data

```bash
# Backup PostgreSQL
docker exec postgres pg_dump -U admin polaris_catalog > polaris_backup.sql
docker exec postgres pg_dump -U admin openmetadata_db > openmetadata_backup.sql

# Backup SeaweedFS data
docker run --rm -v seaweedfs-data:/data -v $(pwd):/backup alpine tar czf /backup/seaweedfs-backup.tar.gz /data
```

### Restore Data

```bash
# Restore PostgreSQL
cat polaris_backup.sql | docker exec -i postgres psql -U admin -d polaris_catalog

# Restore SeaweedFS
docker run --rm -v seaweedfs-data:/data -v $(pwd):/backup alpine tar xzf /backup/seaweedfs-backup.tar.gz -C /data
```

### Update Services

```bash
# Pull latest images
docker compose -f docker-compose-full.yml pull

# Recreate containers
docker compose -f docker-compose-full.yml up -d --force-recreate
```

## Troubleshooting

### Services Won't Start

```bash
# Check logs
docker compose -f docker-compose-full.yml logs <service-name>

# Restart specific service
docker compose -f docker-compose-full.yml restart <service-name>
```

### Cannot Connect to S3

1. Verify SeaweedFS is running: `curl http://localhost:8333`
2. Check credentials in `.env`
3. Ensure path-style access is enabled

### Polaris Catalog Issues

1. Check PostgreSQL connectivity
2. Verify S3 credentials are correct
3. Check Polaris logs: `docker logs polaris-catalog`

### OpenMetadata Not Showing Lineage

1. Verify connector is properly configured
2. Check API connectivity between services
3. Ensure proper authentication tokens

## Production Considerations

⚠️ **Before deploying to production:**

1. **Change all default passwords** in `.env`
2. **Enable authentication** on all services
3. **Configure TLS/SSL** for all endpoints
4. **Set up proper backup strategies**
5. **Configure monitoring and alerting**
6. **Use external PostgreSQL** instead of containerized
7. **Review security groups and network policies**
8. **Set resource limits** on containers

## Next Steps

- Explore sample pipelines in `mage_projects/`
- Read the migration guide: `docs/guides/migration-airflow-to-mage.md`
- Configure additional data sources in OpenMetadata
- Set up automated data quality checks
- Implement CI/CD for pipeline deployments
