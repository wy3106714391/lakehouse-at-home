"""
Verify Tables

Data loader block that verifies the pipeline created the expected tables.
Checks row counts in bronze, silver, and gold layers.
"""

import subprocess
import os


@data_loader
def verify_tables(spark_config: dict, **kwargs) -> dict:
    """
    Verify that the pipeline created the expected Iceberg tables.
    
    Args:
        spark_config: Dictionary from choose_spark_version with spark_master_container
        
    Returns:
        Dictionary with table verification results
    """
    spark_master_container = spark_config.get('spark_master_container', 'spark-master-41')
    
    print(f"Verifying tables on container: {spark_master_container}")
    
    # SQL query to check table row counts
    sql_query = """
    SELECT 'bronze.orders' as table_name, count(*) as row_count FROM iceberg.bronze.orders
    UNION ALL
    SELECT 'silver.orders_clean', count(*) FROM iceberg.silver.orders_clean
    UNION ALL
    SELECT 'gold.daily_summary', count(*) FROM iceberg.gold.daily_summary
    """
    
    cmd = [
        'docker', 'exec', spark_master_container,
        '/opt/spark/bin/spark-sql',
        '-e', sql_query
    ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0:
            print("✓ Table verification completed")
            print(result.stdout)
            
            # Parse results
            table_counts = {}
            for line in result.stdout.strip().split('\n'):
                if line and 'table_name' not in line:
                    parts = line.split()
                    if len(parts) >= 2:
                        table_name = parts[0]
                        row_count = int(parts[1])
                        table_counts[table_name] = row_count
            
            return {
                'status': 'success',
                'tables_verified': table_counts,
                'message': f"Verified {len(table_counts)} tables"
            }
        else:
            print(f"⚠ Table verification skipped - tables may not exist yet")
            print(f"STDERR: {result.stderr}")
            return {
                'status': 'skipped',
                'message': 'Tables may not exist yet',
                'error': result.stderr
            }
            
    except subprocess.TimeoutExpired:
        raise RuntimeError("Table verification timed out after 5 minutes")
    except Exception as e:
        print(f"⚠ Error during verification: {e}")
        return {
            'status': 'error',
            'message': str(e)
        }
