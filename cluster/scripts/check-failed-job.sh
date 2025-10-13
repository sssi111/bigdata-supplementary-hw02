#!/bin/bash

HADOOP_HOME="/opt/hadoop"
JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
HADOOP_USER="hadoop"

if [ -z "$1" ]; then
    echo "Usage: $0 <application_id>"
    echo ""
    echo "Example: $0 application_1760186917153_0006"
    echo ""
    echo "To find recent applications, run:"
    echo "  sudo -u $HADOOP_USER JAVA_HOME=$JAVA_HOME $HADOOP_HOME/bin/yarn application -list -appStates FAILED,FINISHED"
    exit 1
fi

APP_ID=$1

echo "======================================"
echo "Checking logs for application: $APP_ID"
echo "======================================"
echo ""

echo "1. Application information:"
echo "======================================"
sudo -u $HADOOP_USER JAVA_HOME=$JAVA_HOME $HADOOP_HOME/bin/yarn application -status $APP_ID 2>/dev/null

echo ""
echo "2. Aggregated container logs from HDFS:"
echo "======================================"
sudo -u $HADOOP_USER JAVA_HOME=$JAVA_HOME $HADOOP_HOME/bin/yarn logs -applicationId $APP_ID 2>&1 | head -500

echo ""
echo "======================================"
echo "To see full logs, run:"
echo "  sudo -u $HADOOP_USER JAVA_HOME=$JAVA_HOME $HADOOP_HOME/bin/yarn logs -applicationId $APP_ID"
echo "======================================"
