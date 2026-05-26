"""
Compact Small Files

Data exporter block that compacts small files in Iceberg tables.
"""

import subprocess
import os


@data_exporter
def compact_files(**kwargs):
    """
    Compact small files for Iceberg tables.
    
    Rewrites data files to target 128MB file sizes.
    """
    spark_version = os.getenv('SPARK_VERSION', '4.1')
    spark_master_container = 'spark-master-41' if spark_version == '4.1' else 'spark-master'
    
    # Tables to maintain
    tables = [
        "iceberg.bronze.orders",
        "iceberg.silver.orders_clean",
        "iceberg.gold.daily_summary",
    ]
    
    # Target file size: 128MB in bytes
    target_file_size = 134217728
    
    results = []
    
    for table in tables:
        print(f"\nCompacting files for {table}...")
        
        sql_query = f"""
        CALL iceberg.system.rewrite_data_files(
            table => '{table}',
            options => map(
                'target-file-size-bytes', '{target_file_size}',
                'min-input-files', '5'
            )
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
                timeout=900  # 15 minutes for compaction
            )
            
            if result.returncode == 0:
                print(f"✓ Compacted files for {table}")
                results.append({'table': table, 'status': 'success'})
            else:
                print(f"⚠ Skipping {table} - table may not exist or no files to compact")
                results.append({'table': table, 'status': 'skipped', 'error': result.stderr})
                
        except subprocess.TimeoutExpired:
            print(f"✗ Timeout compacting files for {table}")
            results.append({'table': table, 'status': 'timeout'})
        except Exception as e:
            print(f"✗ Error compacting files for {table}: {e}")
            results.append({'table': table, 'status': 'error', 'error': str(e)})
    
    return {'operation': 'compact_files', 'results': results}
