#!/bin/bash
# Setup Mage connections for lakehouse stack
# Run this after Mage is initialized

set -e

echo "Setting up Mage environment variables..."

# Create or update .env file for Mage
cat >> .env << EOF

# Mage Configuration
SPARK_VERSION=4.1
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
EOF

echo "✓ Environment variables configured"

echo ""
echo "Mage setup complete!"
echo "Access Mage UI at: http://localhost:6789"
echo ""
echo "To start Mage:"
echo "  docker-compose -f docker-compose-mage.yml up -d"
echo ""
echo "To run pipelines manually:"
echo "  docker exec mage-api mage run lakehouse_pipelines lakehouse_medallion_pipeline"
echo "  docker exec mage-api mage run lakehouse_pipelines iceberg_maintenance"
