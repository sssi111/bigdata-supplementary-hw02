#!/bin/bash

echo "=================================================="
echo "Verify Spark-Hive Integration"
echo "=================================================="
echo ""

HIVE_HOME="/opt/hive"
JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

# Check if Hive Metastore is running
if ! pgrep -f "HiveMetaStore" > /dev/null; then
    echo "✗ Hive Metastore is not running!"
    echo "  Start it with: bash /opt/hive/scripts/manage-hive-services.sh start metastore"
    exit 1
fi

echo "✓ Hive Metastore is running"
echo ""

# Check if spark_demo database exists
echo "Checking Spark-created databases..."
sudo -u hive JAVA_HOME=$JAVA_HOME $HIVE_HOME/bin/hive -e "SHOW DATABASES" 2>/dev/null | grep spark_demo > /dev/null

if [ $? -eq 0 ]; then
    echo "✓ Found spark_demo database"
    echo ""
    
    echo "Tables in spark_demo database:"
    sudo -u hive JAVA_HOME=$JAVA_HOME $HIVE_HOME/bin/hive -e "USE spark_demo; SHOW TABLES;" 2>/dev/null
    echo ""
    
    # Check sales_data_processed table
    echo "Checking sales_data_processed table..."
    TABLE_EXISTS=$(sudo -u hive JAVA_HOME=$JAVA_HOME $HIVE_HOME/bin/hive -e "USE spark_demo; SHOW TABLES;" 2>/dev/null | grep sales_data_processed)
    
    if [ ! -z "$TABLE_EXISTS" ]; then
        echo "✓ Table sales_data_processed exists"
        echo ""
        
        echo "Table schema:"
        sudo -u hive JAVA_HOME=$JAVA_HOME $HIVE_HOME/bin/hive -e "USE spark_demo; DESCRIBE sales_data_processed;" 2>/dev/null
        echo ""
        
        echo "Partitions:"
        sudo -u hive JAVA_HOME=$JAVA_HOME $HIVE_HOME/bin/hive -e "USE spark_demo; SHOW PARTITIONS sales_data_processed;" 2>/dev/null
        echo ""
        
        echo "Sample data (top 10 records):"
        sudo -u hive JAVA_HOME=$JAVA_HOME $HIVE_HOME/bin/hive -e "USE spark_demo; SELECT * FROM sales_data_processed LIMIT 10;" 2>/dev/null
        echo ""
        
        echo "Record count:"
        sudo -u hive JAVA_HOME=$JAVA_HOME $HIVE_HOME/bin/hive -e "USE spark_demo; SELECT COUNT(*) as total_records FROM sales_data_processed;" 2>/dev/null
        echo ""
        
        echo "Query example - Sales by category:"
        sudo -u hive JAVA_HOME=$JAVA_HOME $HIVE_HOME/bin/hive -e "USE spark_demo; SELECT category, COUNT(*) as count, ROUND(SUM(total_revenue), 2) as revenue FROM sales_data_processed GROUP BY category;" 2>/dev/null
        echo ""
        
        echo "=================================================="
        echo "✓ Spark-Hive integration verified successfully!"
        echo "=================================================="
        echo ""
        echo "You can query the data using:"
        echo "  1. Hive CLI:"
        echo "     sudo -u hive JAVA_HOME=$JAVA_HOME $HIVE_HOME/bin/hive"
        echo ""
        echo "  2. Beeline:"
        echo "     sudo -u hive JAVA_HOME=$JAVA_HOME $HIVE_HOME/bin/beeline -u 'jdbc:hive2://localhost:10000/spark_demo'"
        echo ""
        echo "  3. Direct query:"
        echo "     sudo -u hive JAVA_HOME=$JAVA_HOME $HIVE_HOME/bin/hive -e 'SELECT * FROM spark_demo.sales_data_processed LIMIT 10'"
        echo ""
    else
        echo "⚠ Table sales_data_processed not found"
        echo "  Run the Spark script first: spark-data-processing.py"
    fi
else
    echo "⚠ Database spark_demo not found"
    echo "  Run the Spark script first: spark-data-processing.py"
fi


