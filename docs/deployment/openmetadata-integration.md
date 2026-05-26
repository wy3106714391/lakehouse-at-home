# OpenMetadata 深度集成部署指南

## 概述

本指南介绍如何部署和配置 OpenMetadata 以实现完整的数据血缘与治理闭环。

## 架构组件

### 核心服务
- **OpenMetadata Server**: 元数据管理和血缘追踪核心
- **PostgreSQL**: 存储元数据和配置信息

### 元数据采集器 (Ingestion)
1. **Trino 采集器**: 收集联邦查询层的表结构、视图和查询血缘
2. **StarRocks 采集器**: 收集 OLAP 层的表结构和性能指标
3. **Iceberg 采集器**: 收集数据湖表的版本信息和 Schema 演化
4. **Mage 采集器**: 收集数据管道的执行血缘和依赖关系

### 数据质量引擎
- **Test Runner**: 定时执行数据质量测试并生成报告

## 快速启动

```bash
# 1. 复制环境变量配置
cp .env.example .env

# 2. 启动所有服务（包括 OpenMetadata 采集器）
docker compose -f docker-compose-full.yml up -d

# 3. 查看服务状态
docker compose -f docker-compose-full.yml ps

# 4. 查看 OpenMetadata 日志
docker logs -f openmetadata

# 5. 查看各采集器执行日志
docker logs -f om-ingestion-trino
docker logs -f om-ingestion-starrocks
docker logs -f om-ingestion-iceberg
docker logs -f om-ingestion-mage
docker logs -f om-data-quality
```

## 访问服务

| 服务 | URL | 说明 |
|------|-----|------|
| OpenMetadata UI | http://localhost:8585 | 数据治理和血缘管理 |
| Trino UI | http://localhost:8088 | SQL 查询界面 |
| StarRocks FE | http://localhost:8030 | FE 监控界面 |
| Mage.ai | http://localhost:6789 | 管道编排界面 |

## 配置说明

### 1. Trino 采集配置 (`config/openmetadata/ingestion/trino.yaml`)
```yaml
source:
  type: trino
  serviceName: trino-lakehouse
  connection:
    host: trino-coordinator
    port: 8080
    username: admin
  includeTables: true
  computeLineage:
    enabled: true
    queryLogDuration: 7  # 分析最近 7 天的查询日志
```

### 2. StarRocks 采集配置 (`config/openmetadata/ingestion/starrocks.yaml`)
```yaml
source:
  type: mysql  # StarRocks 使用 MySQL 协议
  serviceName: starrocks-lakehouse
  connection:
    host: starrocks-fe
    port: 9030
    username: root
```

### 3. Iceberg 采集配置 (`config/openmetadata/ingestion/iceberg.yaml`)
```yaml
source:
  type: iceberg
  serviceName: iceberg-lakehouse
  connection:
    catalogName: lakehouse
    uri: http://polaris-catalog:8100
    warehouse: s3a://lakehouse/warehouse
```

### 4. Mage 采集配置 (`config/openmetadata/ingestion/mage.yaml`)
```yaml
source:
  type: mage
  serviceName: mage-pipelines
  connection:
    host: mage-api
    port: 6789
```

### 5. 数据质量测试配置 (`config/openmetadata/tests/test-runner.yaml`)
```yaml
testConfig:
  runAllTests: true
  includeCriticalTests: true
  failOnCritical: false
  notifyOnFailure: true
```

## 功能特性

### 1. 自动化元数据采集
- **定时采集**: 每 6 小时自动更新一次元数据
- **增量采集**: 仅采集变化的元数据，减少资源消耗
- **智能分类**: 自动识别 PII 数据和敏感字段

### 2. 全链路血缘追踪
- **字段级血缘**: 追踪每个字段的来源和转换
- **跨系统血缘**: 连接 Trino → Iceberg → StarRocks → Mage
- **实时血缘**: 管道执行后自动更新血缘关系

### 3. 数据质量管理
- **内置测试**: 提供 20+ 种数据质量测试模板
- **自定义测试**: 支持 SQL 和 Python 自定义测试
- **告警通知**: 测试失败时发送邮件/Slack 通知

### 4. 数据发现与协作
- **全局搜索**: 跨所有数据源的统一搜索
- **数据字典**: 自动生成和维护数据文档
- **团队协作**: 支持评论、标签和关注功能

## 典型使用场景

### 场景 1: 数据问题溯源
```
1. 在 DataEase 中发现报表数据异常
2. 在 OpenMetadata 中搜索相关表
3. 查看血缘图，追溯到上游管道
4. 定位到 Mage 中的具体转换逻辑
5. 修复问题并重新执行管道
```

### 场景 2: 影响分析
```
1. 计划修改 Iceberg 表的 Schema
2. 在 OpenMetadata 中查看该表的使用情况
3. 识别所有依赖该表的下游管道和报表
4. 评估变更影响范围
5. 通知相关人员并制定迁移计划
```

### 场景 3: 数据质量监控
```
1. 为关键表配置数据质量测试
2. Test Runner 每小时自动执行测试
3. 测试失败时触发告警
4. 在 OpenMetadata 中查看测试历史和趋势
5. 持续优化数据质量规则
```

## 高级配置

### 添加 Slack 告警
编辑 `test-runner.yaml`:
```yaml
notifications:
  channels:
    - type: slack
      webhook: https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### 添加自定义数据质量测试
通过 OpenMetadata UI 创建测试:
1. 导航到目标表
2. 点击"Tests"标签
3. 选择测试类型（唯一性、非空、值范围等）
4. 配置测试参数和阈值
5. 保存并激活测试

### 调整采集频率
编辑 `docker-compose-full.yml` 中各采集器的 command:
```yaml
command: >
  bash -c "
    while true; do
      metadata ingest -c /openmetadata/ingestion.yaml || true
      sleep 21600  # 6 小时
    done
  "
```

## 故障排查

### 采集器无法连接数据源
```bash
# 检查网络连通性
docker exec om-ingestion-trino ping trino-coordinator

# 检查数据源服务状态
docker compose -f docker-compose-full.yml ps trino-coordinator

# 查看采集器详细日志
docker logs --tail 100 om-ingestion-trino
```

### OpenMetadata UI 无法访问
```bash
# 检查服务健康状态
curl http://localhost:8585/api/v1/system/version

# 重启服务
docker compose -f docker-compose-full.yml restart openmetadata

# 检查数据库连接
docker logs openmetadata | grep -i "database"
```

### 血缘关系不完整
```bash
# 强制重新采集血缘
docker exec om-ingestion-trino metadata ingest -c /openmetadata/ingestion.yaml --force

# 检查查询日志采集配置
# 确保 Trino 开启了查询日志记录
```

## 最佳实践

1. **定期审查血缘**: 每周审查关键数据链路的血缘完整性
2. **分层质量测试**: 在 Bronze/Silver/Gold 层分别设置不同严格度的测试
3. **文档化**: 为关键表和字段添加业务描述和使用说明
4. **权限管理**: 为不同团队配置适当的访问权限
5. **监控性能**: 监控采集器的执行时间和资源消耗

## 后续扩展

- 集成 Airflow/Dagster 等其他编排工具
- 连接更多数据源（Kafka、Snowflake、BigQuery 等）
- 启用 AI 辅助的数据发现和文档生成
- 集成 DataHub 或其他元数据平台
