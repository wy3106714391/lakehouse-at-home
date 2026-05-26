#!/bin/bash
# PostgreSQL Automated Backup Script
# This script creates compressed backups of all databases and cleans up old backups

set -e

# Configuration
BACKUP_DIR="/backups"
RETENTION_DAYS=7
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DB_USER="${POSTGRES_USER:-admin}"
DB_HOST="${POSTGRES_HOST:-postgres}"

# List of databases to backup (can be overridden by environment variable)
DATABASES="${BACKUP_DATABASES:-polaris openmetadata mage dataease}"

echo "========================================="
echo "PostgreSQL Backup Script"
echo "Timestamp: $TIMESTAMP"
echo "Backup Directory: $BACKUP_DIR"
echo "Retention Days: $RETENTION_DAYS"
echo "Databases: $DATABASES"
echo "========================================="

# Create backup directory if it doesn't exist
mkdir -p "$BACKUP_DIR"

# Function to backup a single database
backup_database() {
    local db_name=$1
    local backup_file="$BACKUP_DIR/${db_name}_${TIMESTAMP}.sql.gz"
    
    echo "Backing up database: $db_name"
    
    # Use pg_dump to create backup and compress with gzip
    PGPASSWORD="${POSTGRES_PASSWORD:-admin}" pg_dump -h "$DB_HOST" -U "$DB_USER" -d "$db_name" | gzip > "$backup_file"
    
    if [ $? -eq 0 ]; then
        local size=$(du -h "$backup_file" | cut -f1)
        echo "✓ Backup successful: $backup_file ($size)"
    else
        echo "✗ Backup failed for database: $db_name"
        return 1
    fi
}

# Function to cleanup old backups
cleanup_old_backups() {
    echo "Cleaning up backups older than $RETENTION_DAYS days..."
    
    find "$BACKUP_DIR" -name "*.sql.gz" -type f -mtime +$RETENTION_DAYS -delete
    
    if [ $? -eq 0 ]; then
        echo "✓ Cleanup completed"
    else
        echo "✗ Cleanup failed"
        return 1
    fi
}

# Function to create a latest symlink
create_latest_symlink() {
    local db_name=$1
    local latest_link="$BACKUP_DIR/${db_name}_latest.sql.gz"
    local latest_backup=$(ls -t "$BACKUP_DIR"/${db_name}_*.sql.gz 2>/dev/null | head -1)
    
    if [ -n "$latest_backup" ]; then
        ln -sf "$latest_backup" "$latest_link"
        echo "✓ Created symlink: $latest_link -> $(basename $latest_backup)"
    fi
}

# Main backup loop
for db in $DATABASES; do
    backup_database "$db"
    create_latest_symlink "$db"
done

# Cleanup old backups
cleanup_old_backups

# Show backup summary
echo ""
echo "========================================="
echo "Backup Summary"
echo "========================================="
ls -lh "$BACKUP_DIR"/*.sql.gz 2>/dev/null || echo "No backups found"
echo ""
echo "Total backup size:"
du -sh "$BACKUP_DIR" | cut -f1
echo "========================================="
echo "Backup completed successfully!"
echo "========================================="
