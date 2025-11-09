#!/bin/bash

echo "=================================================="
echo "Apache Spark Cluster Status Check"
echo "=================================================="
echo ""

SPARK_HOME="/opt/spark"
HADOOP_HOME="/opt/hadoop"
JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

# Check if Spark is installed
if [ ! -d "$SPARK_HOME" ]; then
    echo "✗ Spark is not installed at $SPARK_HOME"
    exit 1
fi

echo "✓ Spark installation found at: $SPARK_HOME"
SPARK_VERSION=$($SPARK_HOME/bin/spark-submit --version 2>&1 | grep "version" | head -1)
echo "  $SPARK_VERSION"
echo ""

# Check YARN status
echo "Checking YARN ResourceManager..."
if pgrep -f "org.apache.hadoop.yarn.server.resourcemanager.ResourceManager" > /dev/null; then
    echo "✓ YARN ResourceManager is running"
else
    echo "✗ YARN ResourceManager is NOT running"
fi
echo ""

# Check HDFS connectivity
echo "Checking HDFS connectivity..."
if sudo -u hadoop $HADOOP_HOME/bin/hdfs dfs -test -d /spark 2>/dev/null; then
    echo "✓ HDFS is accessible"
    echo "  Spark directories in HDFS:"
    sudo -u hadoop $HADOOP_HOME/bin/hdfs dfs -ls /spark 2>/dev/null | tail -n +2
else
    echo "⚠ HDFS /spark directory not found (will be created on first use)"
fi
echo ""

# Check if Hive Metastore is running
echo "Checking Hive Metastore connectivity..."
if pgrep -f "HiveMetaStore" > /dev/null; then
    echo "✓ Hive Metastore is running"
else
    echo "✗ Hive Metastore is NOT running"
    echo "  Start it with: bash /opt/hive/scripts/manage-hive-services.sh start metastore"
fi
echo ""

# Check Spark history server (if configured)
echo "Checking Spark History Server..."
if pgrep -f "org.apache.spark.deploy.history.HistoryServer" > /dev/null; then
    echo "✓ Spark History Server is running"
else
    echo "⚠ Spark History Server is not running (optional)"
fi
echo ""

# Show recent Spark applications on YARN
echo "Recent Spark applications on YARN:"
sudo -u hadoop JAVA_HOME=$JAVA_HOME $HADOOP_HOME/bin/yarn application -list -appStates ALL 2>/dev/null | grep -i spark | head -5 || echo "  No recent Spark applications found"
echo ""

# Check example scripts
echo "Available Spark example scripts:"
if [ -d "$SPARK_HOME/scripts" ]; then
    ls -lh $SPARK_HOME/scripts/*.py 2>/dev/null | awk '{print "  " $9}' || echo "  No Python scripts found"
    ls -lh $SPARK_HOME/scripts/*.sh 2>/dev/null | awk '{print "  " $9}' || echo "  No shell scripts found"
else
    echo "  Scripts directory not found"
fi
echo ""

echo "=================================================="
echo "Spark Environment Information"
echo "=================================================="
echo "SPARK_HOME: $SPARK_HOME"
echo "HADOOP_CONF_DIR: $HADOOP_HOME/etc/hadoop"
echo "JAVA_HOME: $JAVA_HOME"
echo ""
echo "To submit a Spark job:"
echo "  sudo -u spark JAVA_HOME=$JAVA_HOME $SPARK_HOME/bin/spark-submit \\"
echo "    --master yarn \\"
echo "    --deploy-mode client \\"
echo "    --executor-memory 1g \\"
echo "    --executor-cores 1 \\"
echo "    your_script.py"
echo ""
echo "Web interfaces:"
echo "  YARN ResourceManager: http://localhost:8088"
echo "  Spark UI (when job running): http://localhost:4040"
echo ""


