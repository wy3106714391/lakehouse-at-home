# Iceberg 集成指南：Trino 和 StarRocks 查询 Iceberg 表

## 概述

本架构中，**Trino** 和 **StarRocks** 都直接查询存储在 SeaweedFS 上的 Apache Iceberg 表，通过 Polaris Catalog 提供元数据管理。

## 架构设计

```
┌──────────────┐     ┌──────────────┐
│    Trino     │     │  StarRocks   │
│  (联邦查询)   │     │ (OLAP 加速)   │
└──────┬───────┘     └──────┬───────┘
       │                    │
       └────────┬───────────┘
                │
       ┌────────▼────────┐
       │  Polaris Catalog│
       │  (Iceberg REST) │
       └────────┬────────┘
                │
       ┌────────▼────────┐
       │   SeaweedFS     │
       │  (S3 对象存储)    │
       │  - Bronze 层     │
       │  - Silver 层     │
       │  - Gold 层       │
       └─────────────────┘
```

## 1. Trino 配置

### 1.1 连接器配置

Trino 通过 `lakehouse` 连接器访问 Iceberg 表：

**文件**: `config/trino/coordinator/catalog/lakehouse.properties`

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://polaris-catalog:8100
iceberg.rest-catalog.warehouse=s3a://lakehouse/warehouse
hive.s3.endpoint=http://seaweedfs:8333
hive.s3.aws-access-key=admin
hive.s3.aws-secret-key=admin123
hive.s3.path-style-access=true
hive.s3.ssl.enabled=false
```

### 1.2 使用示例

```sql
-- 连接 Trino
docker exec -it trino-coordinator trino --server http://localhost:8080

-- 查看所有 catalog
SHOW CATALOGS;

-- 查看 lakehouse catalog 中的 schemas
SHOW SCHEMAS FROM lakehouse;

-- 创建 schema
CREATE SCHEMA lakehouse.bronze;

-- 查询 Iceberg 表
SELECT * FROM lakehouse.bronze.users LIMIT 10;

-- 创建 Iceberg 表
CREATE TABLE lakehouse.silver.customer_orders (
  order_id BIGINT,
  customer_id BIGINT,
  amount DOUBLE,
  order_date DATE
) WITH (
  format = 'PARQUET',
  partitioning = ARRAY['month(order_date)']
);
```

## 2. StarRocks 配置

### 2.1 启用 Iceberg 支持

StarRocks FE 和 BE 已配置支持外部表查询：

**文件**: `config/starrocks/fe.conf`
```properties
enable_external_table_query = true
enable_iceberg = true
```

### 2.2 创建 Iceberg Catalog

```sql
-- 连接 StarRocks
mysql -h localhost -P 9030 -u root

-- 创建 External Catalog 指向 Polaris
CREATE EXTERNAL CATALOG iceberg_lakehouse
PROPERTIES
(
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.rest.uri" = "http://polaris-catalog:8100",
  "aws.access.key" = "admin",
  "aws.secret.key" = "admin123",
  "s3.endpoint" = "http://seaweedfs:8333",
  "s3.region" = "us-east-1",
  "s3.enable.path.style.access" = "true"
);

-- 查看 catalogs
SHOW CATALOGS;

-- 切换 catalog
USE iceberg_lakehouse;

-- 查看 databases
SHOW DATABASES;

-- 查询 Iceberg 表
SELECT * FROM bronze.users LIMIT 10;

-- 创建物化视图加速查询
CREATE MATERIALIZED VIEW mv_customer_stats
DISTRIBUTED BY HASH(customer_id)
REFRESH ASYNC EVERY(INTERVAL 1 HOUR)
AS SELECT
  customer_id,
  COUNT(*) as order_count,
  SUM(amount) as total_amount
FROM iceberg_lakehouse.silver.customer_orders
GROUP BY customer_id;
```

## 3. 使用场景对比

| 场景 | 推荐引擎 | 原因 |
|------|---------|------|
| 跨源联邦查询 | Trino | 支持连接多个数据源 (MySQL, PostgreSQL, Kafka 等) |
| 即席查询 | Trino | 灵活的 SQL 支持，适合探索性分析 |
| 高性能 OLAP | StarRocks | 向量化执行，物化视图，秒级响应 |
| 实时数据更新 | StarRocks | 支持主键模型，高并发点查 |
| 复杂 ETL | Trino + Spark | Trino 读取，Spark 写入 Iceberg |
| BI 报表加速 | StarRocks | 预计算物化视图，高 QPS 支持 |

## 4. 数据流转示例

### 4.1 Mage 管道写入 Iceberg

```python
# Mage pipeline block
import pyarrow as pa
from pyiceberg.catalog import load_catalog

# 加载 Polaris Catalog
catalog = load_catalog(
    "lakehouse",
    uri="http://polaris-catalog:8100",
    s3.endpoint="http://seaweedfs:8333",
    s3.access_key_id="admin",
    s3.secret_access_key="admin123",
    s3.path_style_access=True
)

# 创建或加载表
table = catalog.create_table(
    "bronze.raw_events",
    schema=pa.schema([
        pa.field("event_id", pa.int64()),
        pa.field("user_id", pa.int64()),
        pa.field("event_type", pa.string()),
        pa.field("timestamp", pa.timestamp('ms'))
    ]),
    partition_spec=None
)

# 写入数据
table.append(dataframe)
```

### 4.2 Trino 查询加工后的数据

```sql
-- 在 Trino 中创建 Silver 层视图
CREATE VIEW lakehouse.silver.user_sessions AS
SELECT
  user_id,
  DATE_TRUNC('hour', timestamp) as session_hour,
  COUNT(*) as event_count,
  ARRAY_AGG(DISTINCT event_type) as event_types
FROM lakehouse.bronze.raw_events
GROUP BY user_id, DATE_TRUNC('hour', timestamp);
```

### 4.3 StarRocks 加速查询

```sql
-- 在 StarRocks 中创建物化视图
CREATE MATERIALIZED VIEW mv_user_hourly_stats
DISTRIBUTED BY HASH(user_id)
REFRESH ASYNC EVERY(INTERVAL 30 MINUTE)
AS SELECT
  user_id,
  session_hour,
  event_count,
  event_types
FROM iceberg_lakehouse.silver.user_sessions;

-- BI 工具直接查询物化视图 (毫秒级响应)
SELECT * FROM mv_user_hourly_stats WHERE user_id = 12345;
```

## 5. OpenMetadata 血缘追踪

OpenMetadata 自动采集 Trino 和 StarRocks 的查询血缘：

1. **Trino 查询血缘**: 记录 `CREATE TABLE AS SELECT` 等操作
2. **StarRocks 物化视图**: 追踪物化视图与基表的依赖关系
3. **端到端血缘**: 从 Mage 管道 → Iceberg 表 → Trino 视图 → StarRocks 物化视图 → DataEase 仪表板

在 OpenMetadata UI (http://localhost:8585) 中查看完整血缘图。

## 6. 性能优化建议

### Trino 优化
- 调整 `query.max-memory-per-node` 根据数据量
- 启用动态过滤 `dynamic_filtering_max_bytes_per_partition`
- 使用统计信息优化查询计划

### StarRocks 优化
- 为高频查询创建物化视图
- 合理设置分桶数 (Bucket)
- 使用 Colocate Join 优化关联查询
- 定期 ANALYZE TABLE 更新统计信息

## 7. 故障排查

### Trino 无法连接 Polaris
```bash
# 检查 Polaris 状态
curl http://localhost:8100/api/management/v1/catalogs

# 检查 Trino 日志
docker logs trino-coordinator | grep -i iceberg
```

### StarRocks 查询 Iceberg 失败
```sql
-- 检查 catalog 状态
SHOW CATALOGS;

-- 手动刷新 catalog
REFRESH CATALOG iceberg_lakehouse;
```

### SeaweedFS 连接问题
```bash
# 检查 S3 连通性
docker exec seaweedfs weed s3.bucket.list

# 检查数据文件
docker exec seaweedfs weed filer.meta.dump
```
