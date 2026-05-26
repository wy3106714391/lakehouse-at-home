"""
Run Pipeline Spark 4.1

Data exporter block that runs the Spark 4.1 pipeline.
Executes spark-submit on the Spark 4.1 master container.
"""

import subprocess
import os


@data_exporter
def run_pipeline_spark41(spark_config: dict, **kwargs):
    """
    Run the Spark 4.1 pipeline using spark-submit.
    
    Args:
        spark_config: Dictionary from choose_spark_version with spark_master_container
    """
    spark_master_container = spark_config.get('spark_master_container', 'spark-master-41')
    
    print(f"Running Spark 4.1 pipeline on container: {spark_master_container}")
    
    # Command to submit Spark job
    cmd = [
        'docker', 'exec', spark_master_container,
        '/opt/spark/bin/spark-submit',
        '/scripts/pipelines/pipeline_spark41.py'
    ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        if result.returncode == 0:
            print("✓ Spark 4.1 pipeline completed successfully")
            print(result.stdout)
            return {'status': 'success', 'spark_version': '4.1'}
        else:
            print(f"✗ Spark 4.1 pipeline failed with return code {result.returncode}")
            print(f"STDERR: {result.stderr}")
            raise RuntimeError(f"Spark 4.1 pipeline failed: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        raise RuntimeError("Spark 4.1 pipeline timed out after 1 hour")
    except Exception as e:
        raise RuntimeError(f"Error running Spark 4.1 pipeline: {e}")
