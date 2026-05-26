-- Initialize databases for Polaris Catalog and OpenMetadata

-- Create database for Polaris Catalog
CREATE DATABASE polaris_catalog;

-- Create database for OpenMetadata
CREATE DATABASE openmetadata_db;

-- Grant privileges (optional, adjust as needed)
GRANT ALL PRIVILEGES ON DATABASE polaris_catalog TO admin;
GRANT ALL PRIVILEGES ON DATABASE openmetadata_db TO admin;
