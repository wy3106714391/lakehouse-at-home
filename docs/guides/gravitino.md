# Apache Gravitino Integration Guide

Apache Gravitino provides a unified metadata layer with enhanced features for multi-catalog support, fine-grained authorization, and cross-engine interoperability.

## Why Apache Gravitino?

| Feature | PostgreSQL JDBC | Unity Catalog OSS | Apache Gravitino |
|---------|----------------|-------------------|------------------|
| Catalog Protocol | JDBC (direct SQL) | REST API (HTTP) | REST API + Multiple backends |
| Supported Formats | Iceberg only | Delta + Iceberg + Hudi | Iceberg + Delta + Hive + JDBC + more |
| Multi-Catalog | No | Single catalog | Federated catalogs |
| Auth | Hardcoded credentials | Credential vending | OAuth/OIDC/Kerberos/Simple |
| Interoperability | Spark only | Spark + DuckDB + Trino | Spark + DuckDB + Trino + Presto + Flink |
| Governance | Manual | Built-in access control | Fine-grained RBAC |
| Geo-Distribution | No | Limited | Yes |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Your Applications                         │
│    (Spark, DuckDB, Trino, Presto, Flink, Python, etc.)      │
└─────────────────────────┬───────────────────────────────────┘
                          │ REST API / Thrift
                          ▼
┌─────────────────────────────────────────────────────────────┐
│              Apache Gravitino Server (port 8090)             │
│  ┌─────────────────┐  ┌─────────────────┐                   │
│  │  Gravitino      │  │  Iceberg REST   │                   │
│  │  Metadata API   │  │  Catalog API    │                   │
│  │  /api/catalogs  │  │  /iceberg/v1/   │                   │
│  │  /api/schemas   │  │                 │                   │
│  │  /api/tables    │  │                 │                   │
│  └─────────────────┘  └─────────────────┘                   │
│                          │                                   │
│              ┌───────────┴───────────┐                      │
│              │   Authorization &     │                      │
│              │   Credential Vending  │                      │
│              └───────────┬───────────┘                      │
└──────────────────────────┼──────────────────────────────────┘
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
    ┌────────────┐  ┌────────────┐  ┌────────────┐
    │ PostgreSQL │  │  SeaweedFS │  │    AWS S3  │
    │ (metadata) │  │   (S3 API) │  │  (storage) │
    └────────────┘  └────────────┘  └────────────┘
```

## Quick Start

### 1. Configure Gravitino

```bash
# Copy example config
cp config/gravitino/gravitino.properties.example config/gravitino/gravitino.properties

# Edit with your credentials
nano config/gravitino/gravitino.properties
```

Update the S3 settings:
```properties
# SeaweedFS S3-compatible storage
gravitino.s3.endpoint=http://seaweedfs:8333
gravitino.s3.accessKey=your_seaweedfs_access_key
gravitino.s3.secretKey=your_seaweedfs_secret_key

# PostgreSQL backend
gravitino.jdbc.url=jdbc:postgresql://postgres:5432/gravitino
gravitino.jdbc.username=postgres
gravitino.jdbc.password=your_postgres_password
```

### 2. Start Gravitino

```bash
./lakehouse start gravitino
```

### 3. Verify It's Running

```bash
# Check status
./lakehouse status

# Test the API
curl http://localhost:8090/api/version

# Run integration tests
python scripts/connectivity/test-gravitino-live.py
```

### 4. Configure Spark to Use Gravitino

Copy the Gravitino Spark config:
```bash
cp config/spark/spark-defaults-gravitino.conf.example config/spark/spark-defaults.conf
```

Or add these settings to your existing config:
```properties
spark.sql.catalog.iceberg                 org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.catalog-impl    org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.iceberg.uri             http://localhost:8091/iceberg/v1/lakehouse
spark.sql.catalog.iceberg.warehouse       lakehouse
spark.sql.catalog.iceberg.token           admin:admin123
```

### 5. Create Tables

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Gravitino-Demo") \
    .getOrCreate()

# Create schema
spark.sql("CREATE SCHEMA IF NOT EXISTS iceberg.bronze")

# Create table
spark.sql("""
    CREATE TABLE iceberg.bronze.orders (
        order_id STRING,
        customer_id STRING,
        total DECIMAL(10,2),
        created_at TIMESTAMP
    ) USING ICEBERG
""")

# Insert data
spark.sql("""
    INSERT INTO iceberg.bronze.orders VALUES
    ('ord-001', 'cust-1', 99.99, current_timestamp()),
    ('ord-002', 'cust-2', 149.99, current_timestamp())
""")

# Query
spark.sql("SELECT * FROM iceberg.bronze.orders").show()
```

## CLI Commands

```bash
# Start Gravitino
./lakehouse start gravitino

# Stop Gravitino
./lakehouse stop gravitino

# View logs
./lakehouse logs gravitino

# Check status
./lakehouse status
```

## REST API Examples

### Get Version

```bash
curl http://localhost:8090/api/version
```

### List Catalogs

```bash
curl http://localhost:8090/api/catalogs
```

### Create Catalog

```bash
curl -X POST http://localhost:8090/api/catalogs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "lakehouse",
    "type": "RELATIONAL",
    "provider": "iceberg",
    "comment": "Main lakehouse catalog",
    "properties": {
      "warehouse": "s3://lakehouse/warehouse",
      "uri": "http://localhost:8091/iceberg/v1/lakehouse"
    }
  }'
```

### List Schemas

```bash
curl http://localhost:8090/api/catalogs/lakehouse/schemas
```

### Create Schema

```bash
curl -X POST http://localhost:8090/api/catalogs/lakehouse/schemas \
  -H "Content-Type: application/json" \
  -d '{
    "name": "bronze",
    "comment": "Bronze layer raw data"
  }'
```

### List Tables

```bash
curl http://localhost:8090/api/catalogs/lakehouse/schemas/bronze/tables
```

## Using with DuckDB

Gravitino works with DuckDB for local analytics:

```sql
-- Install extensions
INSTALL iceberg;
LOAD iceberg;

-- Connect to Iceberg via Gravitino REST Catalog
ATTACH 'iceberg:http://localhost:8091/iceberg/v1/lakehouse?token=admin:admin123' AS lakehouse (TYPE ICEBERG);

-- Query tables
SELECT * FROM lakehouse.bronze.orders;
```

## Using with Trino

Configure Trino connector (`etc/catalog/gravitino.properties`):

```properties
connector.name=gravitino
gravitino.server.uri=http://localhost:8090
gravitino.catalog=lakehouse
```

Then query in Trino:
```sql
SHOW SCHEMAS FROM lakehouse;
SELECT * FROM lakehouse.bronze.orders;
```

## Migration from Unity Catalog

### Side-by-Side Testing

You can run both catalogs simultaneously:

1. Keep your existing Unity Catalog as `unity`
2. Add Gravitino as a new catalog `lakehouse`

```properties
# Existing Unity Catalog
spark.sql.catalog.unity                 org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.unity.catalog-impl    org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.unity.uri             http://localhost:8080/api/2.1/unity-catalog/iceberg

# New Gravitino catalog (different name)
spark.sql.catalog.lakehouse             org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.catalog-impl org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.lakehouse.uri         http://localhost:8091/iceberg/v1/lakehouse
```

Then migrate tables:
```python
# Read from old catalog
df = spark.table("unity.bronze.orders")

# Write to new catalog
df.writeTo("lakehouse.bronze.orders").createOrReplace()
```

### Full Migration

Once validated:

1. Stop applications
2. Update `spark-defaults.conf` to use Gravitino config
3. Restart Spark cluster
4. Verify all queries work
5. Decommission Unity Catalog

## Configuration Reference

### gravitino.properties

| Property | Description | Example |
|----------|-------------|---------|
| `gravitino.server.port` | HTTP port | `8090` |
| `gravitino.storage.backend` | Storage backend | `jdbc` |
| `gravitino.jdbc.url` | PostgreSQL URL | `jdbc:postgresql://localhost:5432/gravitino` |
| `gravitino.authenticator` | Auth type | `simple`, `oauth`, `kerberos` |
| `gravitino.s3.endpoint` | S3 endpoint | `http://localhost:8333` |
| `gravitino.s3.accessKey` | S3 access key | `your_key` |
| `gravitino.s3.secretKey` | S3 secret key | `your_secret` |
| `gravitino.iceberg.rest.port` | Iceberg REST port | `8091` |

### Spark Configuration

| Property | Description |
|----------|-------------|
| `spark.sql.catalog.iceberg.catalog-impl` | Must be `org.apache.iceberg.rest.RESTCatalog` |
| `spark.sql.catalog.iceberg.uri` | Gravitino Iceberg endpoint |
| `spark.sql.catalog.iceberg.warehouse` | Catalog name in Gravitino |
| `spark.sql.catalog.iceberg.token` | Auth token (format: `user:password`) |

## Troubleshooting

### Gravitino Won't Start

```bash
# Check logs
docker logs gravitino

# Verify config exists
ls -la config/gravitino/gravitino.properties

# Check port availability
nc -z localhost 8090 && echo "Port in use" || echo "Port available"

# Check PostgreSQL connection
docker exec gravitino curl -s jdbc:postgresql://postgres:5432/gravitino
```

### Spark Can't Connect

1. Verify Gravitino is running:
   ```bash
   curl http://localhost:8090/api/version
   curl http://localhost:8091/iceberg/v1/lakehouse/config
   ```

2. Check Spark config:
   ```bash
   grep -i "catalog.iceberg" config/spark/spark-defaults.conf
   ```

3. Ensure correct endpoint in Spark:
   ```
   http://localhost:8091/iceberg/v1/lakehouse
   ```

### Tables Not Visible

1. Check schema exists:
   ```bash
   curl http://localhost:8090/api/catalogs/lakehouse/schemas
   ```

2. Verify table was created:
   ```bash
   curl http://localhost:8090/api/catalogs/lakehouse/schemas/bronze/tables
   ```

3. Check Iceberg REST endpoint:
   ```bash
   curl http://localhost:8091/iceberg/v1/lakehouse/namespaces/bronze/tables
   ```

## Advanced Features

### Fine-Grained Authorization

Gravitino supports role-based access control:

```bash
# Create role
curl -X POST http://localhost:8090/api/metalakes/default/roles \
  -H "Content-Type: application/json" \
  -d '{
    "name": "data_engineer",
    "securableObjects": [
      {
        "type": "SCHEMA",
        "privileges": ["READ_TABLE", "WRITE_TABLE"],
        "fullName": "lakehouse.bronze"
      }
    ]
  }'

# Grant role to user
curl -X POST http://localhost:8090/api/metalakes/default/users \
  -H "Content-Type: application/json" \
  -d '{
    "name": "engineer1",
    "roles": ["data_engineer"]
  }'
```

### Multi-Catalog Federation

Access multiple data sources through a single interface:

```bash
# Create Hive catalog
curl -X POST http://localhost:8090/api/catalogs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hive_prod",
    "type": "RELATIONAL",
    "provider": "hive",
    "properties": {
      "metastore.uris": "thrift://hive-metastore:9083"
    }
  }'

# Create MySQL catalog
curl -X POST http://localhost:8090/api/catalogs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql_analytics",
    "type": "RELATIONAL",
    "provider": "jdbc-mysql",
    "properties": {
      "jdbc-url": "jdbc:mysql://mysql:3306/analytics",
      "jdbc-user": "root",
      "jdbc-password": "password"
    }
  }'
```

## Resources

- [Apache Gravitino Documentation](https://gravitino.apache.org/docs/)
- [Apache Gravitino GitHub](https://github.com/apache/gravitino)
- [Iceberg REST Catalog Spec](https://iceberg.apache.org/concepts/catalog/)
- [Gravitino Architecture](https://gravitino.apache.org/docs/architecture)
