#!/bin/bash

HADOOP_HOME="/opt/hadoop"
HADOOP_USER="hadoop"

echo "======================================"
echo "Checking MapReduce Task Logs"
echo "======================================"
echo ""

echo "Available log files in $HADOOP_HOME/logs:"
ls -lh $HADOOP_HOME/logs/*.log 2>/dev/null | tail -10
echo ""

echo "======================================"
echo "Recent ApplicationMaster logs:"
echo "======================================"
sudo find $HADOOP_HOME/logs/userlogs -name "syslog" -type f -mmin -30 2>/dev/null | head -5 | while read log; do
    echo "--- $log ---"
    tail -50 "$log"
    echo ""
done

if [ -z "$(sudo find $HADOOP_HOME/logs/userlogs -name "syslog" -type f -mmin -30 2>/dev/null)" ]; then
    echo "No recent ApplicationMaster logs found (last 30 minutes)"
    echo "Showing any recent container logs:"
    sudo find $HADOOP_HOME/logs/userlogs -name "syslog" -type f 2>/dev/null | head -3 | while read log; do
        echo "--- $log ---"
        tail -30 "$log"
        echo ""
    done
fi

echo ""
echo "======================================"
echo "NodeManager logs (last 50 lines):"
echo "======================================"
NM_LOG=$(ls -t $HADOOP_HOME/logs/hadoop-$HADOOP_USER-nodemanager-*.log 2>/dev/null | head -1)
if [ -n "$NM_LOG" ]; then
    sudo tail -50 "$NM_LOG"
else
    echo "NodeManager log not found. Looking for alternative patterns..."
    NM_LOG=$(ls -t $HADOOP_HOME/logs/*nodemanager*.log 2>/dev/null | head -1)
    if [ -n "$NM_LOG" ]; then
        sudo tail -50 "$NM_LOG"
    else
        echo "No NodeManager logs found"
    fi
fi

echo ""
echo "======================================"
echo "ResourceManager logs (last 30 lines):"
echo "======================================"
RM_LOG=$(ls -t $HADOOP_HOME/logs/hadoop-$HADOOP_USER-resourcemanager-*.log 2>/dev/null | head -1)
if [ -n "$RM_LOG" ]; then
    sudo tail -30 "$RM_LOG"
else
    echo "ResourceManager log not found. Looking for alternative patterns..."
    RM_LOG=$(ls -t $HADOOP_HOME/logs/*resourcemanager*.log 2>/dev/null | head -1)
    if [ -n "$RM_LOG" ]; then
        sudo tail -30 "$RM_LOG"
    else
        echo "No ResourceManager logs found (this is normal if not running on RM node)"
    fi
fi

echo ""
echo "======================================"
echo "YARN containers directory:"
echo "======================================"
sudo ls -la $HADOOP_HOME/data/yarn/local/usercache/ 2>/dev/null || echo "No containers found"

echo ""
echo "======================================"
echo "Container logs for user hadoop:"
echo "======================================"
if [ -d "$HADOOP_HOME/data/yarn/local/usercache/hadoop" ]; then
    echo "Recent containers:"
    sudo find $HADOOP_HOME/data/yarn/local/usercache/hadoop -name "syslog" -type f 2>/dev/null | head -3
else
    echo "No hadoop user containers found"
fi
