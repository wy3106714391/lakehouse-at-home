"""
Check Spark Cluster Availability

Sensor block that verifies the Spark cluster is running.
"""

import subprocess
import os


@sensor
def check_spark_cluster(**kwargs) -> bool:
    """
    Check if Spark cluster is available by running spark-submit --version.
    
    Returns True if Spark is available, False otherwise.
    """
    spark_version = os.getenv('SPARK_VERSION', '4.1')
    spark_master_container = 'spark-master-41' if spark_version == '4.1' else 'spark-master'
    
    cmd = [
        'docker', 'exec', spark_master_container,
        '/opt/spark/bin/spark-submit', '--version'
    ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            print(f"✓ Spark cluster available ({spark_master_container})")
            return True
        else:
            print(f"✗ Spark cluster not available")
            return False
            
    except subprocess.TimeoutExpired:
        print("✗ Timeout checking Spark cluster")
        return False
    except Exception as e:
        print(f"✗ Error checking Spark cluster: {e}")
        return False
