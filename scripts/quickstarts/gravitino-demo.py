#!/usr/bin/env python3
"""
Apache Gravitino Demo Script

This script demonstrates using Apache Gravitino as the unified metadata layer
instead of Unity Catalog or PostgreSQL JDBC catalog.

Prerequisites:
    1. Start Gravitino: ./lakehouse start gravitino
    2. Configure Spark: cp config/spark/spark-defaults-gravitino.conf.example config/spark/spark-defaults.conf
    3. Start Spark: ./lakehouse start spark

Usage:
    # Run via spark-submit
    docker exec spark-master-41 /opt/spark/bin/spark-submit /scripts/gravitino_demo.py

    # Or run locally (requires local Spark with Gravitino config)
    python scripts/gravitino_demo.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from decimal import Decimal
import sys


def create_spark_session():
    """Create SparkSession configured for Apache Gravitino."""

    # Check if we're in a cluster environment (SparkSession may already exist)
    try:
        existing = SparkSession.getActiveSession()
        if existing:
            print("Using existing SparkSession")
            return existing
    except Exception:
        pass

    # Create new session with Gravitino config
    # Gravitino provides Iceberg REST Catalog compatibility
    return SparkSession.builder \
        .appName("Apache Gravitino Demo") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.rest.RESTCatalog") \
        .config("spark.sql.catalog.iceberg.uri", "http://localhost:8091/iceberg/v1/lakehouse") \
        .config("spark.sql.catalog.iceberg.warehouse", "lakehouse") \
        .config("spark.sql.catalog.iceberg.token", "admin:admin123") \
        .getOrCreate()


def setup_schemas(spark):
    """Create medallion architecture schemas."""
    print("\n" + "="*60)
    print("Setting up Medallion Architecture Schemas")
    print("="*60)

    schemas = ["bronze", "silver", "gold"]
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS iceberg.{schema}")
        print(f"  Created schema: iceberg.{schema}")

    # List schemas
    print("\nSchemas in catalog:")
    spark.sql("SHOW SCHEMAS IN iceberg").show()


def create_sample_table(spark):
    """Create and populate a sample orders table."""
    print("\n" + "="*60)
    print("Creating Sample Orders Table")
    print("="*60)

    # Drop if exists
    spark.sql("DROP TABLE IF EXISTS iceberg.bronze.demo_orders")

    # Create table
    spark.sql("""
        CREATE TABLE iceberg.bronze.demo_orders (
            order_id STRING,
            customer_id STRING,
            product_id STRING,
            quantity INT,
            unit_price DECIMAL(10,2),
            total_amount DECIMAL(10,2),
            order_date DATE,
            created_at TIMESTAMP
        ) USING ICEBERG
        PARTITIONED BY (days(order_date))
        TBLPROPERTIES (
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'format-version' = '2'
        )
    """)

    print("  Created table: iceberg.bronze.demo_orders")

    # Insert sample data
    spark.sql("""
        INSERT INTO iceberg.bronze.demo_orders VALUES
        ('ORD-001', 'CUST-001', 'PROD-101', 2, 49.99, 99.98, current_date(), current_timestamp()),
        ('ORD-002', 'CUST-002', 'PROD-102', 1, 149.99, 149.99, current_date(), current_timestamp()),
        ('ORD-003', 'CUST-001', 'PROD-103', 3, 29.99, 89.97, current_date(), current_timestamp()),
        ('ORD-004', 'CUST-003', 'PROD-101', 1, 49.99, 49.99, current_date(), current_timestamp()),
        ('ORD-005', 'CUST-002', 'PROD-104', 2, 79.99, 159.98, current_date(), current_timestamp())
    """)

    print("  Inserted 5 sample records")

    # Query the table
    print("\nTable contents:")
    spark.sql("SELECT * FROM iceberg.bronze.demo_orders").show(truncate=False)


def create_silver_table(spark):
    """Create a silver layer aggregated table."""
    print("\n" + "="*60)
    print("Creating Silver Layer - Customer Summary")
    print("="*60)

    # Drop if exists
    spark.sql("DROP TABLE IF EXISTS iceberg.silver.customer_summary")

    # Create aggregated table
    spark.sql("""
        CREATE TABLE iceberg.silver.customer_summary AS
        SELECT
            customer_id,
            COUNT(*) as total_orders,
            SUM(quantity) as total_items,
            SUM(total_amount) as total_spent,
            AVG(total_amount) as avg_order_value,
            MIN(order_date) as first_order_date,
            MAX(order_date) as last_order_date
        FROM iceberg.bronze.demo_orders
        GROUP BY customer_id
    """)

    print("  Created table: iceberg.silver.customer_summary")

    # Query the table
    print("\nCustomer summary:")
    spark.sql("SELECT * FROM iceberg.silver.customer_summary ORDER BY total_spent DESC").show(truncate=False)


def create_gold_table(spark):
    """Create a gold layer business metrics table."""
    print("\n" + "="*60)
    print("Creating Gold Layer - Daily Metrics")
    print("="*60)

    # Drop if exists
    spark.sql("DROP TABLE IF EXISTS iceberg.gold.daily_metrics")

    # Create metrics table
    spark.sql("""
        CREATE TABLE iceberg.gold.daily_metrics AS
        SELECT
            order_date,
            COUNT(*) as total_orders,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(total_amount) as daily_revenue,
            AVG(total_amount) as avg_order_value,
            SUM(quantity) as total_items_sold
        FROM iceberg.bronze.demo_orders
        GROUP BY order_date
        ORDER BY order_date
    """)

    print("  Created table: iceberg.gold.daily_metrics")

    # Query the table
    print("\nDaily metrics:")
    spark.sql("SELECT * FROM iceberg.gold.daily_metrics ORDER BY order_date DESC").show(truncate=False)


def demonstrate_time_travel(spark):
    """Demonstrate Iceberg time travel features."""
    print("\n" + "="*60)
    print("Time Travel Demonstration")
    print("="*60)

    # Get snapshot history
    print("\nSnapshot history:")
    spark.sql("SELECT * FROM iceberg.bronze.demo_orders.history").show(truncate=False)

    # Show current snapshot
    result = spark.sql("SELECT current_snapshot_id() FROM iceberg.bronze.demo_orders").collect()
    if result:
        print(f"\nCurrent snapshot ID: {result[0][0]}")


def query_via_gravitino_api():
    """Query Gravitino REST API directly."""
    print("\n" + "="*60)
    print("Gravitino REST API Examples")
    print("="*60)

    import subprocess

    # List catalogs
    print("\n1. List Catalogs:")
    result = subprocess.run(
        ["curl", "-s", "http://localhost:8090/api/catalogs"],
        capture_output=True, text=True
    )
    print(f"   Status: {result.returncode}")
    if result.returncode == 0:
        print(f"   Response: {result.stdout[:200]}...")

    # List schemas in lakehouse catalog
    print("\n2. List Schemas in 'lakehouse' catalog:")
    result = subprocess.run(
        ["curl", "-s", "http://localhost:8090/api/catalogs/lakehouse/schemas"],
        capture_output=True, text=True
    )
    print(f"   Status: {result.returncode}")
    if result.returncode == 0:
        print(f"   Response: {result.stdout[:200]}...")


def main():
    """Main demo function."""
    print("="*60)
    print("Apache Gravitino Demo")
    print("="*60)
    print("\nThis demo showcases:")
    print("  - Unified metadata management with Gravitino")
    print("  - Iceberg REST Catalog compatibility")
    print("  - Medallion architecture (Bronze → Silver → Gold)")
    print("  - Time travel queries")
    print("="*60)

    # Create Spark session
    spark = create_spark_session()
    print(f"\n✅ Spark session created: {spark.sparkContext.appName}")

    try:
        # Setup schemas
        setup_schemas(spark)

        # Create bronze layer table
        create_sample_table(spark)

        # Create silver layer table
        create_silver_table(spark)

        # Create gold layer table
        create_gold_table(spark)

        # Demonstrate time travel
        demonstrate_time_travel(spark)

        # Show Gravitino API examples
        query_via_gravitino_api()

        print("\n" + "="*60)
        print("✅ Demo completed successfully!")
        print("="*60)
        print("\nNext steps:")
        print("  1. Explore tables in Spark UI: http://localhost:8082")
        print("  2. View Gravitino UI: http://localhost:8090")
        print("  3. Query with DuckDB/Trino via Gravitino REST API")
        print("  4. Run maintenance tasks: ./lakehouse maintain")

    except Exception as e:
        print(f"\n❌ Error during demo: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Keep session alive for inspection
        print("\nSpark session active. Press Ctrl+C to exit.")
        try:
            import time
            time.sleep(2)
        except KeyboardInterrupt:
            pass
        spark.stop()


if __name__ == "__main__":
    main()
