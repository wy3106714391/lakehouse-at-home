#!/bin/bash
# Setup DolphinScheduler connections and environment variables for lakehouse stack
# Run this after DolphinScheduler is initialized

set -e

echo "Setting up DolphinScheduler environment..."

# Set default environment variables
export SPARK_VERSION="${SPARK_VERSION:-4.1}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

echo "✓ Spark version: $SPARK_VERSION"
echo "✓ Kafka bootstrap servers: $KAFKA_BOOTSTRAP_SERVERS"

# Create configuration file for DolphinScheduler tasks
cat > /opt/dolphinscheduler/config/env.sh << EOF
#!/bin/bash
# DolphinScheduler Lakehouse Environment Configuration

export SPARK_VERSION="${SPARK_VERSION}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS}"

# Spark master URLs
export SPARK_MASTER_40="spark://localhost:7077"
export SPARK_MASTER_41="spark://localhost:7078"

# S3/SeaweedFS settings
export AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY:-}"
export AWS_SECRET_ACCESS_KEY="${S3_SECRET_KEY:-}"
export AWS_ENDPOINT_URL="${S3_ENDPOINT:-http://localhost:8333}"
EOF

chmod +x /opt/dolphinscheduler/config/env.sh

echo "✓ Environment configuration created"

echo ""
echo "DolphinScheduler setup complete!"
echo "Access DolphinScheduler UI at: http://localhost:12345"
echo ""
echo "To import workflows:"
echo "  1. Log in to DolphinScheduler UI"
echo "  2. Navigate to Workflow -> Workflow Definition"
echo "  3. Click 'Import' and select JSON files from /tasks directory"
echo "  4. Configure schedule and enable workflow"
