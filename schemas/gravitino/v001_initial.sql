-- Migration: v001_initial_gravitino
-- Description: Prepare metadata tracking for Apache Gravitino integration
-- Note: Gravitino manages its own backend, this tracks integration metadata

-- Track Gravitino catalog synchronization status
CREATE TABLE IF NOT EXISTS gravitino_catalog_sync (
    id SERIAL PRIMARY KEY,
    iceberg_namespace VARCHAR(255) NOT NULL,
    iceberg_table VARCHAR(255) NOT NULL,
    gravitino_catalog VARCHAR(255) NOT NULL,
    gravitino_schema VARCHAR(255) NOT NULL,
    gravitino_table VARCHAR(255) NOT NULL,
    sync_status VARCHAR(50) DEFAULT 'pending', -- pending, synced, failed, disabled
    last_sync_at TIMESTAMP,
    last_error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(iceberg_namespace, iceberg_table, gravitino_catalog)
);

-- Track Gravitino connection configurations
CREATE TABLE IF NOT EXISTS gravitino_connections (
    id SERIAL PRIMARY KEY,
    connection_name VARCHAR(255) UNIQUE NOT NULL,
    api_endpoint VARCHAR(500) NOT NULL,
    catalog_name VARCHAR(255) NOT NULL,
    auth_type VARCHAR(50) NOT NULL, -- simple, oauth, kerberos
    credential_secret_name VARCHAR(255), -- Reference to secret manager
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Track fileset storage locations managed by Gravitino
CREATE TABLE IF NOT EXISTS gravitino_filesets (
    id SERIAL PRIMARY KEY,
    catalog_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    fileset_name VARCHAR(255) NOT NULL,
    storage_path VARCHAR(1000) NOT NULL,
    storage_type VARCHAR(50) DEFAULT 's3', -- s3, oss, gcs, abfs
    is_managed BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(catalog_name, schema_name, fileset_name)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_gravitino_sync_status ON gravitino_catalog_sync(sync_status);
CREATE INDEX IF NOT EXISTS idx_gravitino_sync_iceberg ON gravitino_catalog_sync(iceberg_namespace, iceberg_table);
CREATE INDEX IF NOT EXISTS idx_gravitino_filesets_catalog ON gravitino_filesets(catalog_name, schema_name);

-- Insert default connection placeholder
INSERT INTO gravitino_connections
    (connection_name, api_endpoint, catalog_name, auth_type, is_active)
VALUES
    ('local-gravitino', 'http://localhost:8090/api', 'lakehouse', 'simple', false)
ON CONFLICT (connection_name) DO NOTHING;

COMMENT ON TABLE gravitino_catalog_sync IS 'Tracks synchronization between Iceberg and Gravitino managed tables';
COMMENT ON TABLE gravitino_connections IS 'Gravitino connection configurations (credentials stored externally)';
COMMENT ON TABLE gravitino_filesets IS 'Fileset storage locations managed by Gravitino';
