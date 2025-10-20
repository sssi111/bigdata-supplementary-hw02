#!/bin/bash

# Demo Script for Partitioned Tables in Hive
# This script demonstrates creating and working with partitioned tables

HIVE_HOME="/opt/hive"
HADOOP_HOME="/opt/hadoop"
JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
JDBC_HIVE="jdbc:hive2://192.168.1.15:10000"

echo "=========================================="
echo "Hive Partitioned Tables Demo"
echo "=========================================="
echo "Date: $(date)"
echo ""

# Create demo database
echo "1. Creating demo database..."
sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "CREATE DATABASE IF NOT EXISTS demo_db; USE demo_db;"
echo "✓ Demo database created"
echo ""

# Create sample data directory
echo "2. Creating sample data..."
SAMPLE_DIR="/tmp/hive_demo_data"
mkdir -p "$SAMPLE_DIR"

# Generate sample sales data
echo "Generating sample sales data..."
cat > "$SAMPLE_DIR/sales_2024_01.csv" << EOF
1,Product A,100.50,2024-01-15
2,Product B,250.75,2024-01-16
3,Product C,75.25,2024-01-17
4,Product A,120.00,2024-01-18
5,Product B,300.00,2024-01-19
EOF

cat > "$SAMPLE_DIR/sales_2024_02.csv" << EOF
6,Product C,85.50,2024-02-01
7,Product A,110.25,2024-02-02
8,Product B,275.00,2024-02-03
9,Product C,90.75,2024-02-04
10,Product A,135.50,2024-02-05
EOF

cat > "$SAMPLE_DIR/sales_2024_03.csv" << EOF
11,Product B,320.00,2024-03-01
12,Product C,95.25,2024-03-02
13,Product A,125.75,2024-03-03
14,Product B,280.50,2024-03-04
15,Product C,88.00,2024-03-05
EOF

echo "✓ Sample data generated"
echo ""

# Create partitioned sales table
echo "3. Creating partitioned sales table..."
sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "
USE demo_db;
DROP TABLE IF EXISTS sales_data;
CREATE TABLE sales_data (
    id int,
    product string,
    amount double,
    sale_date string
)
PARTITIONED BY (
    year int,
    month int
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/demo_db.db/sales_data'
TBLPROPERTIES (
    'comment' = 'Demo partitioned sales table',
    'created_by' = 'demo-script'
);
"
echo "✓ Partitioned sales table created"
echo ""

# Load data into partitions
echo "4. Loading data into partitions..."

# Load 2024-01 data
echo "Loading 2024-01 data..."
sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -put "$SAMPLE_DIR/sales_2024_01.csv" /tmp/
sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "
USE demo_db;
LOAD DATA INPATH '/tmp/sales_2024_01.csv' INTO TABLE sales_data PARTITION (year=2024, month=1);
"

# Load 2024-02 data
echo "Loading 2024-02 data..."
sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -put "$SAMPLE_DIR/sales_2024_02.csv" /tmp/
sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "
USE demo_db;
LOAD DATA INPATH '/tmp/sales_2024_02.csv' INTO TABLE sales_data PARTITION (year=2024, month=2);
"

# Load 2024-03 data
echo "Loading 2024-03 data..."
sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -put "$SAMPLE_DIR/sales_2024_03.csv" /tmp/
sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "
USE demo_db;
LOAD DATA INPATH '/tmp/sales_2024_03.csv' INTO TABLE sales_data PARTITION (year=2024, month=3);
"

echo "✓ Data loaded into all partitions"
echo ""

# Show table structure and partitions
echo "5. Table structure and partitions..."
echo "Table structure:"
sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "USE demo_db; DESCRIBE sales_data;"
echo ""

echo "Available partitions:"
sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "USE demo_db; SHOW PARTITIONS sales_data;"
echo ""

# Demonstrate queries
echo "6. Demonstrating queries..."

echo "Total sales by month:"
sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "
USE demo_db;
SELECT year, month, COUNT(*) as record_count, SUM(amount) as total_sales
FROM sales_data
GROUP BY year, month
ORDER BY year, month;
"
echo ""

echo "Product sales summary:"
sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "
USE demo_db;
SELECT product, COUNT(*) as sales_count, SUM(amount) as total_amount, AVG(amount) as avg_amount
FROM sales_data
GROUP BY product
ORDER BY total_amount DESC;
"
echo ""

echo "Sales for January 2024:"
sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "
USE demo_db;
SELECT id, product, amount, sale_date
FROM sales_data
WHERE year=2024 AND month=1
ORDER BY id;
"
echo ""

# Demonstrate partition pruning
echo "7. Demonstrating partition pruning..."
echo "Query with partition pruning (only scans January data):"
sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "
USE demo_db;
EXPLAIN EXTENDED
SELECT COUNT(*) FROM sales_data WHERE year=2024 AND month=1;
"
echo ""

# Create another partitioned table for user events
echo "8. Creating user events table..."
sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "
USE demo_db;
DROP TABLE IF EXISTS user_events;
CREATE TABLE user_events (
    user_id int,
    event_type string,
    event_data string,
    timestamp bigint
)
PARTITIONED BY (
    date string
)
STORED AS ORC
LOCATION '/user/hive/warehouse/demo_db.db/user_events'
TBLPROPERTIES (
    'comment' = 'Demo partitioned user events table',
    'created_by' = 'demo-script'
);
"
echo "✓ User events table created"
echo ""

# Generate and load user events data
echo "9. Generating and loading user events data..."
cat > "$SAMPLE_DIR/events_2024-01-15.csv" << EOF
1,login,{"ip":"192.168.1.100"},1705305600
2,click,{"page":"home","element":"button"},1705305660
3,view,{"page":"product","id":"123"},1705305720
4,logout,{"session":"abc123"},1705305780
EOF

cat > "$SAMPLE_DIR/events_2024-01-16.csv" << EOF
5,login,{"ip":"192.168.1.101"},1705392000
6,purchase,{"product":"Product A","amount":100.50},1705392060
7,view,{"page":"checkout"},1705392120
8,logout,{"session":"def456"},1705392180
EOF

# Load events data
sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -put "$SAMPLE_DIR/events_2024-01-15.csv" /tmp/
sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -put "$SAMPLE_DIR/events_2024-01-16.csv" /tmp/

sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "
USE demo_db;
LOAD DATA INPATH '/tmp/events_2024-01-15.csv' INTO TABLE user_events PARTITION (date='2024-01-15');
LOAD DATA INPATH '/tmp/events_2024-01-16.csv' INTO TABLE user_events PARTITION (date='2024-01-16');
"
echo "✓ User events data loaded"
echo ""

# Show events data
echo "10. User events data:"
sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -u $JDBC_HIVE -e "
USE demo_db;
SELECT user_id, event_type, event_data, from_unixtime(timestamp) as event_time, date
FROM user_events
ORDER BY timestamp;
"
echo ""

# Clean up
echo "11. Cleaning up temporary files..."
rm -rf "$SAMPLE_DIR"
sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -rm -f /tmp/sales_2024_*.csv
sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -rm -f /tmp/events_2024-*.csv
echo "✓ Cleanup completed"
echo ""

echo "=========================================="
echo "Demo completed successfully!"
echo "=========================================="
echo ""
echo "Created tables:"
echo "  - demo_db.sales_data (partitioned by year, month)"
echo "  - demo_db.user_events (partitioned by date)"
echo ""
echo "You can now:"
echo "1. Query the tables using Hive CLI"
echo "2. Connect via HiveServer2 (port 10000)"
echo "3. Monitor via Hive Web UI (port 10002)"
echo "4. Use the management scripts for service control"
echo ""
echo "Example queries:"
echo "  USE demo_db;"
echo "  SHOW TABLES;"
echo "  SELECT * FROM sales_data WHERE year=2024 AND month=1;"
echo "  SHOW PARTITIONS sales_data;"
