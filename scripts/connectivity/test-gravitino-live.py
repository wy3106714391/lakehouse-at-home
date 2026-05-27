#!/usr/bin/env python3
"""
Apache Gravitino Live Integration Test

Tests Apache Gravitino with live services:
- Gravitino REST API operations
- Spark with Iceberg REST Catalog configuration
- Table creation via Gravitino-managed catalog
- Data operations via Spark
- Cross-client access verification

Prerequisites:
    ./lakehouse start gravitino
    # Or: ./lakehouse start all && ./lakehouse start gravitino

Usage:
    # Direct Python (uses requests for REST API)
    python scripts/test-gravitino-live.py

    # Via spark-submit (requires Gravitino-configured Spark)
    docker exec spark-master-41 /opt/spark/bin/spark-submit /scripts/test-gravitino-live.py
"""

import sys
import json
import subprocess
from datetime import datetime

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    print("Warning: requests not installed. Using curl fallback.")

from pyspark.sql import SparkSession
from pyspark.sql import functions as f


# Configuration
GRAVITINO_HOST = "localhost"
GRAVITINO_API_PORT = 8090
GRAVITINO_ICEBERG_PORT = 8091
GRAVITINO_BASE_URL = f"http://{GRAVITINO_HOST}:{GRAVITINO_API_PORT}"
GRAVITINO_ICEBERG_URL = f"http://{GRAVITINO_HOST}:{GRAVITINO_ICEBERG_PORT}/iceberg/v1/lakehouse"

# Test catalog/schema/table names
TEST_CATALOG = "lakehouse"
TEST_SCHEMA = "test_live"
TEST_TABLE = "orders"


def check_gravitino_health():
    """Check if Gravitino is running and healthy."""
    print("\n" + "=" * 60)
    print("Test 1: Gravitino Health Check")
    print("=" * 60)

    url = f"{GRAVITINO_BASE_URL}/api/version"

    if REQUESTS_AVAILABLE:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                print(f"  Gravitino is running")
                print(f"  Version: {data.get('gravitinoVersion', 'unknown')}")
                print(f"  Compile version: {data.get('compileVersion', 'unknown')}")
                print("  ✅ Health check passed")
                return True
            else:
                print(f"  ❌ Unexpected status: {response.status_code}")
                return False
        except requests.exceptions.ConnectionError:
            print(f"  ❌ Cannot connect to Gravitino at {GRAVITINO_BASE_URL}")
            print("     Run: ./lakehouse start gravitino")
            return False
        except Exception as e:
            print(f"  ❌ Health check failed: {e}")
            return False
    else:
        # Fallback to curl
        try:
            result = subprocess.run(
                ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", url],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.stdout == "200":
                print("  ✅ Gravitino is running (curl check)")
                return True
            else:
                print(f"  ❌ Gravitino returned: {result.stdout}")
                return False
        except Exception as e:
            print(f"  ❌ Health check failed: {e}")
            return False


def list_catalogs():
    """List available catalogs in Gravitino."""
    print("\n" + "=" * 60)
    print("Test 2: List Catalogs")
    print("=" * 60)

    url = f"{GRAVITINO_BASE_URL}/api/catalogs"

    if REQUESTS_AVAILABLE:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                catalogs = data.get('catalogs', [])
                print(f"  Found {len(catalogs)} catalog(s):")
                for cat in catalogs:
                    print(f"    - {cat.get('name')} ({cat.get('type', 'unknown')})")
                return True
            else:
                print(f"  ❌ Failed to list catalogs: {response.status_code}")
                return False
        except Exception as e:
            print(f"  ❌ Error: {e}")
            return False
    else:
        # Fallback to curl
        try:
            result = subprocess.run(
                ["curl", "-s", url],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                print(f"  Response: {result.stdout[:200]}...")
                return True
            else:
                print(f"  ❌ Curl failed: {result.stderr}")
                return False
        except Exception as e:
            print(f"  ❌ Error: {e}")
            return False


def create_test_schema_via_spark(spark):
    """Create test schema using Spark."""
    print("\n" + "=" * 60)
    print("Test 3: Create Schema via Spark")
    print("=" * 60)

    try:
        # Create schema
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS iceberg.{TEST_SCHEMA}")
        print(f"  Created schema: iceberg.{TEST_SCHEMA}")

        # Verify schema exists
        schemas = [row[0] for row in spark.sql("SHOW SCHEMAS IN iceberg").collect()]
        if TEST_SCHEMA in schemas:
            print(f"  ✅ Schema '{TEST_SCHEMA}' verified")
            return True
        else:
            print(f"  ❌ Schema '{TEST_SCHEMA}' not found")
            return False

    except Exception as e:
        print(f"  ❌ Error creating schema: {e}")
        return False


def create_test_table_via_spark(spark):
    """Create test table using Spark."""
    print("\n" + "=" * 60)
    print("Test 4: Create Table via Spark")
    print("=" * 60)

    try:
        # Drop table if exists
        spark.sql(f"DROP TABLE IF EXISTS iceberg.{TEST_SCHEMA}.{TEST_TABLE}")

        # Create table
        spark.sql(f"""
            CREATE TABLE iceberg.{TEST_SCHEMA}.{TEST_TABLE} (
                id LONG,
                name STRING,
                value DOUBLE,
                created_at TIMESTAMP
            ) USING ICEBERG
            TBLPROPERTIES (
                'format-version' = '2'
            )
        """)
        print(f"  Created table: iceberg.{TEST_SCHEMA}.{TEST_TABLE}")

        # Insert test data
        spark.sql(f"""
            INSERT INTO iceberg.{TEST_SCHEMA}.{TEST_TABLE} VALUES
            (1, 'test_record_1', 100.5, current_timestamp()),
            (2, 'test_record_2', 200.75, current_timestamp()),
            (3, 'test_record_3', 300.25, current_timestamp())
        """)
        print(f"  Inserted 3 test records")

        # Verify data
        count = spark.sql(f"SELECT COUNT(*) FROM iceberg.{TEST_SCHEMA}.{TEST_TABLE}").collect()[0][0]
        if count == 3:
            print(f"  ✅ Table verified with {count} records")
            return True
        else:
            print(f"  ❌ Expected 3 records, got {count}")
            return False

    except Exception as e:
        print(f"  ❌ Error creating table: {e}")
        import traceback
        traceback.print_exc()
        return False


def query_test_table(spark):
    """Query the test table."""
    print("\n" + "=" * 60)
    print("Test 5: Query Test Table")
    print("=" * 60)

    try:
        df = spark.sql(f"SELECT * FROM iceberg.{TEST_SCHEMA}.{TEST_TABLE} ORDER BY id")
        print(f"  Query results:")
        df.show(truncate=False)
        print(f"  ✅ Query successful")
        return True
    except Exception as e:
        print(f"  ❌ Query failed: {e}")
        return False


def verify_via_iceberg_rest_api():
    """Verify table exists via Iceberg REST API."""
    print("\n" + "=" * 60)
    print("Test 6: Verify via Iceberg REST API")
    print("=" * 60)

    url = f"{GRAVITINO_ICEBERG_URL}/namespaces/{TEST_SCHEMA}/tables/{TEST_TABLE}"

    if REQUESTS_AVAILABLE:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print(f"  ✅ Table verified via REST API")
                return True
            elif response.status_code == 404:
                print(f"  ❌ Table not found via REST API")
                return False
            else:
                print(f"  ❌ Unexpected status: {response.status_code}")
                return False
        except Exception as e:
            print(f"  ❌ REST API check failed: {e}")
            return False
    else:
        # Fallback to curl
        try:
            result = subprocess.run(
                ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", url],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.stdout == "200":
                print(f"  ✅ Table verified via REST API (curl)")
                return True
            else:
                print(f"  ❌ Table not found: {result.stdout}")
                return False
        except Exception as e:
            print(f"  ❌ Error: {e}")
            return False


def cleanup(spark):
    """Clean up test resources."""
    print("\n" + "=" * 60)
    print("Cleanup")
    print("=" * 60)

    try:
        spark.sql(f"DROP TABLE IF EXISTS iceberg.{TEST_SCHEMA}.{TEST_TABLE}")
        print(f"  Dropped table: {TEST_TABLE}")

        spark.sql(f"DROP SCHEMA IF EXISTS iceberg.{TEST_SCHEMA}")
        print(f"  Dropped schema: {TEST_SCHEMA}")

        print(f"  ✅ Cleanup completed")
        return True
    except Exception as e:
        print(f"  ⚠️  Cleanup warning: {e}")
        return False


def main():
    """Run all integration tests."""
    print("=" * 60)
    print("Apache Gravitino Live Integration Tests")
    print("=" * 60)
    print(f"Gravitino API URL: {GRAVITINO_BASE_URL}")
    print(f"Iceberg REST URL: {GRAVITINO_ICEBERG_URL}")
    print(f"Test catalog: {TEST_CATALOG}")
    print(f"Test schema: {TEST_SCHEMA}")
    print(f"Test table: {TEST_TABLE}")
    print("=" * 60)

    results = []

    # Test 1: Health check (no Spark needed)
    results.append(("Health Check", check_gravitino_health()))

    # Test 2: List catalogs (no Spark needed)
    results.append(("List Catalogs", list_catalogs()))

    # Create Spark session for remaining tests
    print("\n" + "=" * 60)
    print("Creating Spark Session")
    print("=" * 60)

    try:
        spark = SparkSession.builder \
            .appName("Gravitino Integration Test") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.rest.RESTCatalog") \
            .config("spark.sql.catalog.iceberg.uri", GRAVITINO_ICEBERG_URL) \
            .config("spark.sql.catalog.iceberg.warehouse", TEST_CATALOG) \
            .config("spark.sql.catalog.iceberg.token", "admin:admin123") \
            .getOrCreate()

        print(f"✅ Spark session created: {spark.sparkContext.appName}")

        # Test 3: Create schema
        results.append(("Create Schema", create_test_schema_via_spark(spark)))

        # Test 4: Create table
        results.append(("Create Table", create_test_table_via_spark(spark)))

        # Test 5: Query table
        results.append(("Query Table", query_test_table(spark)))

        # Test 6: Verify via REST API
        results.append(("REST API Verify", verify_via_iceberg_rest_api()))

        # Cleanup
        results.append(("Cleanup", cleanup(spark)))

        spark.stop()

    except Exception as e:
        print(f"\n❌ Spark test suite failed: {e}")
        import traceback
        traceback.print_exc()
        results.append(("Spark Tests", False))

    # Print summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status}: {test_name}")

    print(f"\nTotal: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
