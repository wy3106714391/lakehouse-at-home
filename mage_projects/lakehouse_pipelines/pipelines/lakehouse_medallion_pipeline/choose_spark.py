"""
Choose Spark Version

Transformer block that determines which Spark version to use.
Returns the spark version and container name for downstream blocks.
"""

import os


@transformer
def choose_spark_version(*args, **kwargs):
    """
    Determine which Spark version to use based on environment variable.
    
    Returns a dictionary with spark_version and spark_master_container.
    """
    spark_version = os.getenv('SPARK_VERSION', '4.1')
    
    if spark_version == '4.1':
        spark_master_container = 'spark-master-41'
    else:
        spark_master_container = 'spark-master'
    
    print(f"Using Spark version: {spark_version}")
    print(f"Spark master container: {spark_master_container}")
    
    return {
        'spark_version': spark_version,
        'spark_master_container': spark_master_container,
        'run_spark41': spark_version == '4.1',
        'run_spark40': spark_version != '4.1',
    }
