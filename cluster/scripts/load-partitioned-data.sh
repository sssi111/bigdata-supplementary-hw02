#!/bin/bash

# Load Partitioned Data Script for Hive
# This script loads data into partitioned Hive tables with automatic partition management

HIVE_HOME="/opt/hive"
HADOOP_HOME="/opt/hadoop"
JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
JDBC_HIVE="jdbc:hive2://192.168.1.15:10000"

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  -d, --database DATABASE    Database name (default: default)"
    echo "  -t, --table TABLE          Table name"
    echo "  -f, --file FILE            Source data file (local or HDFS path)"
    echo "  -p, --partition PARTITION  Partition values (e.g., 'year=2024,month=01,day=15')"
    echo "  -s, --source SOURCE        Source type (local, hdfs, s3) (default: local)"
    echo "  -o, --overwrite            Overwrite existing data in partition"
    echo "  -a, --append               Append data to existing partition"
    echo "  -c, --create-partition     Create partition if it doesn't exist"
    echo "  -b, --batch-size SIZE      Batch size for loading (default: 1000)"
    echo "  -h, --help                 Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 -t sales_data -f /tmp/sales_2024_01.csv -p 'year=2024,month=01'"
    echo "  $0 -d analytics -t user_events -f hdfs:///data/events/2024/01/15/ -p 'date=2024-01-15' -s hdfs"
    echo "  $0 -t logs -f /var/log/app.log -p 'year=2024,month=01,day=15' -c -a"
}

# Default values
DATABASE="default"
TABLE=""
FILE=""
PARTITION_VALUES=""
SOURCE_TYPE="local"
OVERWRITE=false
APPEND=false
CREATE_PARTITION=false
BATCH_SIZE=1000

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
        -f|--file)
            FILE="$2"
            shift 2
            ;;
        -p|--partition)
            PARTITION_VALUES="$2"
            shift 2
            ;;
        -s|--source)
            SOURCE_TYPE="$2"
            shift 2
            ;;
        -o|--overwrite)
            OVERWRITE=true
            shift
            ;;
        -a|--append)
            APPEND=true
            shift
            ;;
        -c|--create-partition)
            CREATE_PARTITION=true
            shift
            ;;
        -b|--batch-size)
            BATCH_SIZE="$2"
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

if [ -z "$FILE" ]; then
    echo "Error: Source file is required"
    usage
    exit 1
fi

if [ -z "$PARTITION_VALUES" ]; then
    echo "Error: Partition values are required"
    usage
    exit 1
fi

# Validate source type
if [[ "$SOURCE_TYPE" != "local" && "$SOURCE_TYPE" != "hdfs" && "$SOURCE_TYPE" != "s3" ]]; then
    echo "Error: Invalid source type. Must be 'local', 'hdfs', or 's3'"
    usage
    exit 1
fi

echo "=========================================="
echo "Loading Partitioned Data into Hive"
echo "=========================================="
echo "Database: $DATABASE"
echo "Table: $TABLE"
echo "Source file: $FILE"
echo "Source type: $SOURCE_TYPE"
echo "Partition values: $PARTITION_VALUES"
echo "Overwrite: $OVERWRITE"
echo "Append: $APPEND"
echo "Create partition: $CREATE_PARTITION"
echo "Batch size: $BATCH_SIZE"
echo ""

# Check if table exists
echo "Checking if table exists..."
TABLE_EXISTS=$(sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "USE ${DATABASE}; SHOW TABLES LIKE '${TABLE}';" 2>/dev/null | grep -c "$TABLE")

if [ "$TABLE_EXISTS" -eq 0 ]; then
    echo "✗ Table '$TABLE' does not exist in database '$DATABASE'"
    echo "Please create the table first using create-partitioned-table.sh"
    exit 1
fi

echo "✓ Table '$TABLE' exists"
echo ""

# Check if source file exists
echo "Checking source file..."
case $SOURCE_TYPE in
    "local")
        if [ ! -f "$FILE" ]; then
            echo "✗ Local file '$FILE' does not exist"
            exit 1
        fi
        echo "✓ Local file exists"
        ;;
    "hdfs")
        if ! sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -test -e "$FILE" 2>/dev/null; then
            echo "✗ HDFS file '$FILE' does not exist"
            exit 1
        fi
        echo "✓ HDFS file exists"
        ;;
    "s3")
        echo "⚠ S3 source type not fully implemented - assuming file exists"
        ;;
esac
echo ""

# Prepare HDFS staging area
STAGING_DIR="/tmp/hive_staging/${DATABASE}/${TABLE}/$(date +%Y%m%d_%H%M%S)"
echo "Preparing HDFS staging area: $STAGING_DIR"

# Create staging directory
sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "$STAGING_DIR"

if [ $? -ne 0 ]; then
    echo "✗ Failed to create staging directory"
    exit 1
fi

echo "✓ Staging directory created"
echo ""

# Copy data to HDFS staging area
echo "Copying data to HDFS staging area..."
case $SOURCE_TYPE in
    "local")
        sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -put "$FILE" "$STAGING_DIR/"
        ;;
    "hdfs")
        sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -cp "$FILE" "$STAGING_DIR/"
        ;;
    "s3")
        echo "⚠ S3 copy not implemented - assuming data is already in HDFS"
        ;;
esac

if [ $? -ne 0 ]; then
    echo "✗ Failed to copy data to staging area"
    exit 1
fi

echo "✓ Data copied to staging area"
echo ""

# Create partition if requested
if [ "$CREATE_PARTITION" = true ]; then
    echo "Creating partition if it doesn't exist..."
    PARTITION_SQL="ALTER TABLE ${DATABASE}.${TABLE} ADD IF NOT EXISTS PARTITION (${PARTITION_VALUES});"
    
    sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "USE ${DATABASE}; $PARTITION_SQL"
    
    if [ $? -eq 0 ]; then
        echo "✓ Partition created or already exists"
    else
        echo "✗ Failed to create partition"
        exit 1
    fi
    echo ""
fi

# Prepare LOAD DATA statement
LOAD_MODE=""
if [ "$OVERWRITE" = true ]; then
    LOAD_MODE="OVERWRITE"
elif [ "$APPEND" = true ]; then
    LOAD_MODE="INTO"
else
    LOAD_MODE="INTO"
fi

# Get the actual file name in staging area
STAGED_FILE=$(sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -ls "$STAGING_DIR" | tail -1 | awk '{print $NF}')

LOAD_SQL="
USE ${DATABASE};
LOAD DATA INPATH '${STAGED_FILE}' ${LOAD_MODE} TABLE ${TABLE} PARTITION (${PARTITION_VALUES});
"

echo "Executing data load..."
echo "SQL: $LOAD_SQL"
echo ""

# Execute LOAD DATA statement
sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "$LOAD_SQL"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Data loaded successfully into partition (${PARTITION_VALUES})"
    echo ""
    
    # Show partition information
    echo "Partition information:"
    sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "USE ${DATABASE}; SHOW PARTITIONS ${TABLE};"
    echo ""
    
    # Show row count in partition
    echo "Row count in partition:"
    PARTITION_WHERE=$(echo "$PARTITION_VALUES" | sed 's/,/ AND /g')
    sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "USE ${DATABASE}; SELECT COUNT(*) FROM ${TABLE} WHERE ${PARTITION_WHERE};"
    echo ""
    
    # Clean up staging area
    echo "Cleaning up staging area..."
    sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -rm -r "$STAGING_DIR"
    
    if [ $? -eq 0 ]; then
        echo "✓ Staging area cleaned up"
    else
        echo "⚠ Warning: Failed to clean up staging area"
    fi
    
    echo ""
    echo "=========================================="
    echo "Data loading completed successfully!"
    echo "=========================================="
    echo ""
    echo "Data loaded into:"
    echo "  Database: $DATABASE"
    echo "  Table: $TABLE"
    echo "  Partition: $PARTITION_VALUES"
    echo ""
    echo "Next steps:"
    echo "1. Query the data using Hive CLI or HiveServer2"
    echo "2. Monitor table usage in Hive Web UI (port 10002)"
    echo "3. Load more data into other partitions as needed"
    
else
    echo ""
    echo "✗ Failed to load data into partition"
    echo "Please check the error messages above and try again."
    
    # Clean up staging area on failure
    echo "Cleaning up staging area..."
    sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -rm -r "$STAGING_DIR"
    exit 1
fi
