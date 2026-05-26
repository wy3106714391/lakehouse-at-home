# Migrating from Airflow to Mage.ai

This guide explains how the Airflow DAGs have been migrated to Mage.ai pipelines.

## Overview

The following Airflow DAGs have been converted to Mage pipelines:

| Airflow DAG | Mage Pipeline | Location |
|-------------|---------------|----------|
| `lakehouse_medallion_pipeline` | `lakehouse_medallion_pipeline` | `mage_projects/lakehouse_pipelines/pipelines/lakehouse_medallion_pipeline/` |
| `iceberg_maintenance` | `iceberg_maintenance` | `mage_projects/lakehouse_pipelines/pipelines/iceberg_maintenance/` |

## Architecture Changes

### Airflow Components → Mage Equivalents

| Airflow | Mage | Notes |
|---------|------|-------|
| DAG | Pipeline | Defined in `metadata.yaml` |
| Operator (BashOperator) | Block (data_exporter, transformer, etc.) | Blocks are Python functions with decorators |
| Sensor | Sensor block | Returns boolean to indicate readiness |
| Variable | Environment variable | Use `os.getenv()` in blocks |
| Connection | Environment variable | Configure in `.env` file |
| Webserver (port 8085) | UI (port 6789) | Access at http://localhost:6789 |
| Scheduler | Built-in scheduler | Run `mage start` to start |

## File Structure

### Airflow Structure
```
dags/
├── lakehouse_medallion_pipeline.py
└── iceberg_maintenance.py
```

### Mage Structure
```
mage_projects/lakehouse_pipelines/
├── metadata.yaml
└── pipelines/
    ├── lakehouse_medallion_pipeline/
    │   ├── metadata.yaml
    │   ├── check_kafka.py
    │   ├── choose_spark.py
    │   ├── run_pipeline_spark41.py
    │   ├── run_pipeline_spark40.py
    │   └── verify_tables.py
    └── iceberg_maintenance/
        ├── metadata.yaml
        ├── check_spark.py
        ├── expire_snapshots.py
        ├── remove_orphans.py
        └── compact_files.py
```

## Block Types Mapping

### Lakehouse Medallion Pipeline

| Airflow Task | Mage Block | Block Type | Description |
|--------------|------------|------------|-------------|
| `check_kafka_availability` | `check_kafka` | sensor | Checks Kafka connectivity |
| `choose_spark_version` | `choose_spark` | transformer | Determines Spark version |
| `run_pipeline_spark41` | `run_pipeline_spark41` | data_exporter | Runs Spark 4.1 pipeline |
| `run_pipeline_spark40` | `run_pipeline_spark40` | data_exporter | Runs Spark 4.0 pipeline |
| `verify_tables` | `verify_tables` | data_loader | Verifies table creation |

### Iceberg Maintenance Pipeline

| Airflow Task | Mage Block | Block Type | Description |
|--------------|------------|------------|-------------|
| `check_spark_cluster` | `check_spark` | sensor | Checks Spark availability |
| `expire_snapshots_*` | `expire_snapshots` | data_exporter | Expires old snapshots |
| `remove_orphans_*` | `remove_orphans` | data_exporter | Removes orphan files |
| `compact_files_*` | `compact_files` | data_exporter | Compacts small files |

## Running Pipelines

### Starting Mage

```bash
# Start Mage with docker-compose
docker-compose -f docker-compose-mage.yml up -d

# Access UI at http://localhost:6789
```

### Running Pipelines Manually

```bash
# Run lakehouse medallion pipeline
docker exec mage-api mage run lakehouse_pipelines lakehouse_medallion_pipeline

# Run iceberg maintenance
docker exec mage-api mage run lakehouse_pipelines iceberg_maintenance
```

### Scheduling Pipelines

In the Mage UI:
1. Navigate to the pipeline
2. Click "Schedule" 
3. Configure the schedule (e.g., `@daily` or cron expression)

For Iceberg maintenance (daily at 3 AM):
- Cron: `0 3 * * *`

## Configuration

### Environment Variables

Set these in your `.env` file:

```bash
# Spark configuration
SPARK_VERSION=4.1
SPARK_MASTER_40=spark://localhost:7077
SPARK_MASTER_41=spark://localhost:7078
SPARK_HOME=/opt/spark

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Database for Mage metadata
DB_CONNECTION_URL=postgresql://postgres:password@localhost:5432/mage_data
```

## Key Differences

### 1. Code Organization
- **Airflow**: Single Python file per DAG
- **Mage**: One file per block (task), organized in directories

### 2. Data Passing
- **Airflow**: XComs for small data, shared storage for large data
- **Mage**: Automatic data passing between blocks via function return values

### 3. Branching Logic
- **Airflow**: `BranchPythonOperator` to choose execution path
- **Mage**: Conditional execution based on returned values from upstream blocks

### 4. Error Handling
- **Airflow**: `trigger_rule` parameter (e.g., `none_failed_min_one_success`)
- **Mage**: Try-catch blocks within Python code, return status dictionaries

### 5. Sensors
- **Airflow**: Dedicated sensor classes with poke_interval
- **Mage**: Sensor blocks that return boolean values

## Migration Benefits

1. **Modern UI**: Mage provides a more intuitive interface for pipeline development
2. **Notebook Integration**: Built-in Jupyter notebook support for exploratory work
3. **Data Testing**: Native data quality testing capabilities
4. **Observability**: Enhanced logging and monitoring features
5. **Simpler Development**: Pure Python blocks without complex operator hierarchies

## Troubleshooting

### Common Issues

1. **Container networking**: Ensure `network_mode: "host"` is set for local development
2. **Spark submit**: Verify SPARK_HOME is correctly set in the container
3. **Database**: Create the `mage_data` database before starting Mage

### Logs

```bash
# View Mage logs
docker logs mage-api

# View specific pipeline run logs
docker exec mage-api mage logs lakehouse_pipelines lakehouse_medallion_pipeline
```

## Next Steps

1. Test pipelines manually in the Mage UI
2. Configure schedules for automated runs
3. Set up alerts for pipeline failures
4. Migrate any additional Airflow DAGs following the same pattern
