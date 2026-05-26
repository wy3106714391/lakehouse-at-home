"""
Remove Orphan Files

Data exporter block that removes orphan files from Iceberg tables.
"""

import subprocess
import os
from datetime import datetime, timedelta


@data_exporter
def remove_orphans(**kwargs):
    """
    Remove orphan files for Iceberg tables.
    
    Removes files older than 3 days that are not referenced by any snapshot.
    """
    spark_version = os.getenv('SPARK_VERSION', '4.1')
    spark_master_container = 'spark-master-41' if spark_version == '4.1' else 'spark-master'
    
    # Tables to maintain
    tables = [
        "iceberg.bronze.orders",
        "iceberg.silver.orders_clean",
        "iceberg.gold.daily_summary",
    ]
    
    # Calculate timestamp for 3 days ago
    three_days_ago = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')
    
    results = []
    
    for table in tables:
        print(f"\nRemoving orphan files for {table}...")
        
        sql_query = f"""
        CALL iceberg.system.remove_orphan_files(
            table => '{table}',
            older_than => TIMESTAMP '{three_days_ago}'
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
                timeout=600  # 10 minutes for orphan removal
            )
            
            if result.returncode == 0:
                print(f"✓ Removed orphan files for {table}")
                results.append({'table': table, 'status': 'success'})
            else:
                print(f"⚠ Skipping {table} - table may not exist")
                results.append({'table': table, 'status': 'skipped', 'error': result.stderr})
                
        except subprocess.TimeoutExpired:
            print(f"✗ Timeout removing orphan files for {table}")
            results.append({'table': table, 'status': 'timeout'})
        except Exception as e:
            print(f"✗ Error removing orphan files for {table}: {e}")
            results.append({'table': table, 'status': 'error', 'error': str(e)})
    
    return {'operation': 'remove_orphans', 'results': results}
