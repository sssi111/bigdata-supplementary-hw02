#!/bin/bash

# Create Partitioned Table Script for Hive
# This script creates partitioned tables in Hive with various partitioning strategies

HIVE_HOME="/opt/hive"
JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
JDBC_HIVE="jdbc:hive2://192.168.1.15:10000"

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  -d, --database DATABASE    Database name (default: default)"
    echo "  -t, --table TABLE          Table name"
    echo "  -p, --partition PARTITION  Partition column (e.g., 'year int, month int, day int')"
    echo "  -c, --columns COLUMNS      Table columns (e.g., 'id int, name string, value double')"
    echo "  -s, --storage STORAGE      Storage format (parquet, orc, textfile) (default: parquet)"
    echo "  -l, --location LOCATION    HDFS location for table data"
    echo "  -h, --help                 Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 -t sales_data -p 'year int, month int' -c 'id int, product string, amount double'"
    echo "  $0 -d analytics -t user_events -p 'date string' -c 'user_id int, event string, timestamp bigint' -s orc"
    echo "  $0 -t logs -p 'year int, month int, day int' -c 'log_id string, message string, level string' -s textfile"
}

# Default values
DATABASE="default"
TABLE=""
PARTITION_COLUMNS=""
TABLE_COLUMNS=""
STORAGE_FORMAT="parquet"
LOCATION=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--database)
            DATABASE="$2"
            shift 2
            ;;
        -t|--table)
            TABLE="$2"
            shift 2
            ;;
        -p|--partition)
            PARTITION_COLUMNS="$2"
            shift 2
            ;;
        -c|--columns)
            TABLE_COLUMNS="$2"
            shift 2
            ;;
        -s|--storage)
            STORAGE_FORMAT="$2"
            shift 2
            ;;
        -l|--location)
            LOCATION="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate required parameters
if [ -z "$TABLE" ]; then
    echo "Error: Table name is required"
    usage
    exit 1
fi

if [ -z "$PARTITION_COLUMNS" ]; then
    echo "Error: Partition columns are required"
    usage
    exit 1
fi

if [ -z "$TABLE_COLUMNS" ]; then
    echo "Error: Table columns are required"
    usage
    exit 1
fi

# Set default location if not provided
if [ -z "$LOCATION" ]; then
    LOCATION="/user/hive/warehouse/${DATABASE}.db/${TABLE}"
fi

echo "=========================================="
echo "Creating Partitioned Table in Hive"
echo "=========================================="
echo "Database: $DATABASE"
echo "Table: $TABLE"
echo "Partition columns: $PARTITION_COLUMNS"
echo "Table columns: $TABLE_COLUMNS"
echo "Storage format: $STORAGE_FORMAT"
echo "Location: $LOCATION"
echo ""

# Create HiveQL statement
HIVEQL="
-- Create database if not exists
CREATE DATABASE IF NOT EXISTS ${DATABASE};

-- Use the database
USE ${DATABASE};

-- Drop table if exists
DROP TABLE IF EXISTS ${TABLE};

-- Create partitioned table
CREATE TABLE ${TABLE} (
    ${TABLE_COLUMNS}
)
PARTITIONED BY (
    ${PARTITION_COLUMNS}
)
STORED AS ${STORAGE_FORMAT}
LOCATION '${LOCATION}'
TBLPROPERTIES (
    'comment' = 'Partitioned table created by automated script',
    'created_by' = 'hive-deployment-script',
    'created_date' = '$(date)'
);
"

echo "Executing HiveQL:"
echo "$HIVEQL"
echo ""

# Execute HiveQL
echo "Creating table..."
sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "$HIVEQL"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Table '$TABLE' created successfully in database '$DATABASE'"
    echo ""
    
    # Show table structure
    echo "Table structure:"
    sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "USE ${DATABASE}; DESCRIBE ${TABLE};"
    echo ""
    
    # Show partitions (will be empty initially)
    echo "Partitions (initially empty):"
    sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "USE ${DATABASE}; SHOW PARTITIONS ${TABLE};"
    echo ""
    
    echo "=========================================="
    echo "Table creation completed successfully!"
    echo "=========================================="
    echo ""
    echo "Next steps:"
    echo "1. Load data into the table using load-partitioned-data.sh"
    echo "2. Query the table using Hive CLI or HiveServer2"
    echo "3. Monitor table usage in Hive Web UI (port 10002)"
else
    echo ""
    echo "✗ Failed to create table '$TABLE'"
    echo "Please check the error messages above and try again."
    exit 1
fi
