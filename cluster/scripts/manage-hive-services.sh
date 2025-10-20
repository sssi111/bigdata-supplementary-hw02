#!/bin/bash

# Hive Services Management Script
# This script provides easy management of Hive services (Metastore and HiveServer2)

HIVE_HOME="/opt/hive"
JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

# Function to display usage
usage() {
    echo "Usage: $0 [COMMAND] [SERVICE]"
    echo ""
    echo "Commands:"
    echo "  start       Start Hive services"
    echo "  stop        Stop Hive services"
    echo "  restart     Restart Hive services"
    echo "  status      Show status of Hive services"
    echo "  logs        Show logs of Hive services"
    echo ""
    echo "Services:"
    echo "  metastore   Hive Metastore service only"
    echo "  hiveserver2 HiveServer2 service only"
    echo "  all         All Hive services (default)"
    echo ""
    echo "Examples:"
    echo "  $0 start all"
    echo "  $0 stop metastore"
    echo "  $0 restart hiveserver2"
    echo "  $0 status"
    echo "  $0 logs metastore"
}

# Function to start Metastore
start_metastore() {
    echo "Starting Hive Metastore..."
    
    # Check if already running
    if pgrep -f "HiveMetaStore" > /dev/null; then
        echo "Hive Metastore is already running"
        return 0
    fi
    
    # Start Metastore
    sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" --service metastore > "$HIVE_HOME/logs/metastore.log" 2>&1 &
    
    # Wait for startup
    echo "Waiting for Metastore to start..."
    for i in {1..30}; do
        if pgrep -f "HiveMetaStore" > /dev/null; then
            echo "✓ Hive Metastore started successfully"
            return 0
        fi
        sleep 2
    done
    
    echo "✗ Failed to start Hive Metastore"
    return 1
}

# Function to start HiveServer2
start_hiveserver2() {
    echo "Starting HiveServer2..."
    
    # Check if already running
    if pgrep -f "HiveServer2" > /dev/null; then
        echo "HiveServer2 is already running"
        return 0
    fi
    
    # Start HiveServer2
    sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/hive" --service hiveserver2 > "$HIVE_HOME/logs/hiveserver2.log" 2>&1 &
    
    # Wait for startup
    echo "Waiting for HiveServer2 to start..."
    for i in {1..30}; do
        if pgrep -f "HiveServer2" > /dev/null; then
            echo "✓ HiveServer2 started successfully"
            return 0
        fi
        sleep 2
    done
    
    echo "✗ Failed to start HiveServer2"
    return 1
}

# Function to stop Metastore
stop_metastore() {
    echo "Stopping Hive Metastore..."
    
    if ! pgrep -f "HiveMetaStore" > /dev/null; then
        echo "Hive Metastore is not running"
        return 0
    fi
    
    # Stop Metastore
    sudo -u hive pkill -f "HiveMetaStore"
    
    # Wait for shutdown
    for i in {1..15}; do
        if ! pgrep -f "HiveMetaStore" > /dev/null; then
            echo "✓ Hive Metastore stopped successfully"
            return 0
        fi
        sleep 1
    done
    
    echo "✗ Failed to stop Hive Metastore gracefully, forcing..."
    sudo -u hive pkill -9 -f "HiveMetaStore"
    sleep 2
    
    if ! pgrep -f "HiveMetaStore" > /dev/null; then
        echo "✓ Hive Metastore stopped (forced)"
        return 0
    else
        echo "✗ Failed to stop Hive Metastore"
        return 1
    fi
}

# Function to stop HiveServer2
stop_hiveserver2() {
    echo "Stopping HiveServer2..."
    
    if ! pgrep -f "HiveServer2" > /dev/null; then
        echo "HiveServer2 is not running"
        return 0
    fi
    
    # Stop HiveServer2
    sudo -u hive pkill -f "HiveServer2"
    
    # Wait for shutdown
    for i in {1..15}; do
        if ! pgrep -f "HiveServer2" > /dev/null; then
            echo "✓ HiveServer2 stopped successfully"
            return 0
        fi
        sleep 1
    done
    
    echo "✗ Failed to stop HiveServer2 gracefully, forcing..."
    sudo -u hive pkill -9 -f "HiveServer2"
    sleep 2
    
    if ! pgrep -f "HiveServer2" > /dev/null; then
        echo "✓ HiveServer2 stopped (forced)"
        return 0
    else
        echo "✗ Failed to stop HiveServer2"
        return 1
    fi
}

# Function to show status
show_status() {
    echo "=========================================="
    echo "Hive Services Status"
    echo "=========================================="
    echo "Date: $(date)"
    echo ""
    
    # Metastore status
    if pgrep -f "HiveMetaStore" > /dev/null; then
        echo "✓ Hive Metastore: RUNNING (PID: $(pgrep -f 'HiveMetaStore'))"
        echo "  Port: 9083"
    else
        echo "✗ Hive Metastore: NOT RUNNING"
    fi
    
    # HiveServer2 status
    if pgrep -f "HiveServer2" > /dev/null; then
        echo "✓ HiveServer2: RUNNING (PID: $(pgrep -f 'HiveServer2'))"
        echo "  Port: 10000 (Thrift), 10002 (Web UI)"
    else
        echo "✗ HiveServer2: NOT RUNNING"
    fi
    
    echo ""
    
    # Port status
    echo "Port Status:"
    echo "  9083 (Metastore):"
    netstat -tlnp | grep :9083 || echo "    Not listening"
    
    echo "  10000 (HiveServer2 Thrift):"
    netstat -tlnp | grep :10000 || echo "    Not listening"
    
    echo "  10002 (HiveServer2 Web UI):"
    netstat -tlnp | grep :10002 || echo "    Not listening"
    
    echo ""
    echo "=========================================="
}

# Function to show logs
show_logs() {
    local service=$1
    
    case $service in
        "metastore")
            echo "=========================================="
            echo "Hive Metastore Logs (last 50 lines)"
            echo "=========================================="
            if [ -f "$HIVE_HOME/logs/metastore.log" ]; then
                tail -50 "$HIVE_HOME/logs/metastore.log"
            else
                echo "Log file not found: $HIVE_HOME/logs/metastore.log"
            fi
            ;;
        "hiveserver2")
            echo "=========================================="
            echo "HiveServer2 Logs (last 50 lines)"
            echo "=========================================="
            if [ -f "$HIVE_HOME/logs/hiveserver2.log" ]; then
                tail -50 "$HIVE_HOME/logs/hiveserver2.log"
            else
                echo "Log file not found: $HIVE_HOME/logs/hiveserver2.log"
            fi
            ;;
        "all")
            echo "=========================================="
            echo "All Hive Services Logs"
            echo "=========================================="
            echo ""
            echo "--- Metastore Logs ---"
            if [ -f "$HIVE_HOME/logs/metastore.log" ]; then
                tail -25 "$HIVE_HOME/logs/metastore.log"
            else
                echo "Log file not found: $HIVE_HOME/logs/metastore.log"
            fi
            echo ""
            echo "--- HiveServer2 Logs ---"
            if [ -f "$HIVE_HOME/logs/hiveserver2.log" ]; then
                tail -25 "$HIVE_HOME/logs/hiveserver2.log"
            else
                echo "Log file not found: $HIVE_HOME/logs/hiveserver2.log"
            fi
            ;;
        *)
            echo "Unknown service: $service"
            echo "Available services: metastore, hiveserver2, all"
            exit 1
            ;;
    esac
}

# Main script logic
COMMAND=$1
SERVICE=${2:-all}

case $COMMAND in
    "start")
        case $SERVICE in
            "metastore")
                start_metastore
                ;;
            "hiveserver2")
                start_hiveserver2
                ;;
            "all")
                start_metastore && start_hiveserver2
                ;;
            *)
                echo "Unknown service: $SERVICE"
                usage
                exit 1
                ;;
        esac
        ;;
    "stop")
        case $SERVICE in
            "metastore")
                stop_metastore
                ;;
            "hiveserver2")
                stop_hiveserver2
                ;;
            "all")
                stop_hiveserver2 && stop_metastore
                ;;
            *)
                echo "Unknown service: $SERVICE"
                usage
                exit 1
                ;;
        esac
        ;;
    "restart")
        case $SERVICE in
            "metastore")
                stop_metastore && start_metastore
                ;;
            "hiveserver2")
                stop_hiveserver2 && start_hiveserver2
                ;;
            "all")
                stop_hiveserver2 && stop_metastore && start_metastore && start_hiveserver2
                ;;
            *)
                echo "Unknown service: $SERVICE"
                usage
                exit 1
                ;;
        esac
        ;;
    "status")
        show_status
        ;;
    "logs")
        if [ -z "$SERVICE" ] || [ "$SERVICE" = "all" ]; then
            show_logs "all"
        else
            show_logs "$SERVICE"
        fi
        ;;
    *)
        echo "Unknown command: $COMMAND"
        usage
        exit 1
        ;;
esac
