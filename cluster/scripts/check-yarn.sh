#!/bin/bash

HADOOP_HOME="/opt/hadoop"
JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
HADOOP_USER="hadoop"

echo "======================================"
echo "YARN Cluster Status Check"
echo "======================================"
echo ""

echo "1. Checking YARN processes..."
ps aux | grep -E 'ResourceManager|NodeManager|JobHistoryServer' | grep -v grep
echo ""

echo "2. YARN node status..."
sudo -u $HADOOP_USER JAVA_HOME=$JAVA_HOME $HADOOP_HOME/bin/yarn node -list -all
echo ""

echo "3. YARN application list..."
sudo -u $HADOOP_USER JAVA_HOME=$JAVA_HOME $HADOOP_HOME/bin/yarn application -list
echo ""

echo "======================================"
echo "Web Interfaces:"
echo "======================================"
echo "ResourceManager: http://192.168.1.15:8088"
echo "NodeManager (node 1): http://192.168.1.15:8042"
echo "NodeManager (node 2): http://192.168.1.16:8042"
echo "NodeManager (node 3): http://192.168.1.17:8042"
echo "JobHistoryServer: http://192.168.1.15:19888"
echo "======================================"
