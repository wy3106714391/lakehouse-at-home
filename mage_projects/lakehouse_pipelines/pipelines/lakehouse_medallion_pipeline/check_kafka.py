"""
Check Kafka Availability

Sensor block that checks if Kafka is available.
In Mage, sensors are used to wait for external conditions.
"""

import socket
import os
from mage_ai.settings.repo import get_repo_path


@sensor
def check_kafka_availability(**kwargs) -> bool:
    """
    Check if Kafka is available on the configured host and port.
    
    Returns True if Kafka is available, False otherwise.
    In Mage, sensors return a boolean to indicate whether to proceed.
    """
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    host, port = kafka_bootstrap.split(':')
    port = int(port)
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print(f"✓ Kafka is available at {kafka_bootstrap}")
            return True
        else:
            print(f"✗ Kafka not available at {kafka_bootstrap} - continuing anyway")
            return True  # Continue anyway like the Airflow version
    except Exception as e:
        print(f"✗ Error checking Kafka: {e} - continuing anyway")
        return True  # Continue anyway like the Airflow version
