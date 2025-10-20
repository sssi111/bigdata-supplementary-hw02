#!/bin/bash

# Hive Cluster Status Check Script
# This script checks the status of Hive services and provides diagnostic information

HIVE_HOME="/opt/hive"
HADOOP_HOME="/opt/hadoop"
JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

echo "=========================================="
echo "Hive Cluster Status Check"
echo "=========================================="
echo "Date: $(date)"
echo ""

# Check Java
echo "1. Checking Java installation..."
if [ -d "$JAVA_HOME" ]; then
    echo "   ✓ Java found at: $JAVA_HOME"
    java -version 2>&1 | head -1
else
    echo "   ✗ Java not found at: $JAVA_HOME"
fi
echo ""

# Check Hadoop services
echo "2. Checking Hadoop services..."
echo "   ResourceManager status:"
if pgrep -f "ResourceManager" > /dev/null; then
    echo "   ✓ ResourceManager is running"
else
    echo "   ✗ ResourceManager is not running"
fi

echo "   NodeManager status:"
if pgrep -f "NodeManager" > /dev/null; then
    echo "   ✓ NodeManager is running"
else
    echo "   ✗ NodeManager is not running"
fi

echo "   JobHistoryServer status:"
if pgrep -f "JobHistoryServer" > /dev/null; then
    echo "   ✓ JobHistoryServer is running"
else
    echo "   ✗ JobHistoryServer is not running"
fi
echo ""

# Check Hive services
echo "3. Checking Hive services..."
echo "   Hive Metastore status:"
if pgrep -f "HiveMetaStore" > /dev/null; then
    echo "   ✓ Hive Metastore is running"
    echo "   ✓ Metastore listening on port 9083"
else
    echo "   ✗ Hive Metastore is not running"
fi

echo "   Hive Server2 status:"
if pgrep -f "HiveServer2" > /dev/null; then
    echo "   ✓ Hive Server2 is running"
    echo "   ✓ HiveServer2 listening on port 10000"
    echo "   ✓ HiveServer2 Web UI on port 10002"
else
    echo "   ✗ Hive Server2 is not running"
fi
echo ""

# Check MySQL
echo "4. Checking MySQL (Metastore database)..."
if systemctl is-active --quiet mysql; then
    echo "   ✓ MySQL service is running"
else
    echo "   ✗ MySQL service is not running"
fi
echo ""

# Check HDFS connectivity
echo "5. Checking HDFS connectivity..."
if [ -f "$HADOOP_HOME/bin/hdfs" ]; then
    echo "   Testing HDFS connection..."
    sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfsadmin -report | head -5
    echo ""
    
    echo "   Checking Hive directories in HDFS..."
    sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -ls /user/hive/warehouse 2>/dev/null || echo "   ✗ Hive warehouse directory not found"
    sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -ls /tmp/hive 2>/dev/null || echo "   ✗ Hive scratch directory not found"
else
    echo "   ✗ HDFS client not found"
fi
echo ""

# Check Hive configuration
echo "6. Checking Hive configuration..."
if [ -f "$HIVE_HOME/conf/hive-site.xml" ]; then
    echo "   ✓ hive-site.xml found"
else
    echo "   ✗ hive-site.xml not found"
fi

if [ -f "$HIVE_HOME/conf/tez-site.xml" ]; then
    echo "   ✓ tez-site.xml found"
else
    echo "   ✗ tez-site.xml not found"
fi
echo ""

# Test Hive connection
echo "7. Testing Hive connection..."
if [ -f "$HIVE_HOME/bin/hive" ]; then
    echo "   Testing Hive CLI connection..."
    timeout 30 sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" -e "SHOW DATABASES;" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "   ✓ Hive CLI connection successful"
    else
        echo "   ✗ Hive CLI connection failed"
    fi
else
    echo "   ✗ Hive CLI not found"
fi
echo ""

# Check Tez
echo "8. Checking Tez installation..."
if [ -d "$HADOOP_HOME/tez" ]; then
    echo "   ✓ Tez installation found"
    if sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -test -e /apps/tez/tez.tar.gz 2>/dev/null; then
        echo "   ✓ Tez tar.gz found in HDFS"
    else
        echo "   ✗ Tez tar.gz not found in HDFS"
    fi
else
    echo "   ✗ Tez installation not found"
fi
echo ""

# Port status
echo "9. Checking port status..."
echo "   Port 9083 (Metastore):"
netstat -tlnp | grep :9083 || echo "   ✗ Port 9083 not listening"

echo "   Port 10000 (HiveServer2):"
netstat -tlnp | grep :10000 || echo "   ✗ Port 10000 not listening"

echo "   Port 10002 (HiveServer2 Web UI):"
netstat -tlnp | grep :10002 || echo "   ✗ Port 10002 not listening"
echo ""

echo "=========================================="
echo "Hive Cluster Status Check Complete"
echo "=========================================="
