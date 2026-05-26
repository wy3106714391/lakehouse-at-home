"""
Expire Old Snapshots

Data exporter block that expires old Iceberg table snapshots.
"""

import subprocess
import os
from datetime import datetime, timedelta


@data_exporter
def expire_snapshots(**kwargs):
    """
    Expire old snapshots for Iceberg tables.
    
    Keeps the last 5 snapshots and expires anything older than 7 days.
    """
    spark_version = os.getenv('SPARK_VERSION', '4.1')
    spark_master_container = 'spark-master-41' if spark_version == '4.1' else 'spark-master'
    
    # Tables to maintain
    tables = [
        "iceberg.bronze.orders",
        "iceberg.silver.orders_clean",
        "iceberg.gold.daily_summary",
    ]
    
    # Calculate timestamp for 7 days ago
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    
    results = []
    
    for table in tables:
        print(f"\nExpiring snapshots for {table}...")
        
        sql_query = f"""
        CALL iceberg.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{seven_days_ago}',
            retain_last => 5
        )
        """
        
        cmd = [
            'docker', 'exec', spark_master_container,
            '/opt/spark/bin/spark-sql', '-e', sql_query
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                print(f"✓ Expired snapshots for {table}")
                results.append({'table': table, 'status': 'success'})
            else:
                print(f"⚠ Skipping {table} - table may not exist")
                results.append({'table': table, 'status': 'skipped', 'error': result.stderr})
                
        except subprocess.TimeoutExpired:
            print(f"✗ Timeout expiring snapshots for {table}")
            results.append({'table': table, 'status': 'timeout'})
        except Exception as e:
            print(f"✗ Error expiring snapshots for {table}: {e}")
            results.append({'table': table, 'status': 'error', 'error': str(e)})
    
    return {'operation': 'expire_snapshots', 'results': results}
