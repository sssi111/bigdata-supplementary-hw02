#!/bin/bash

# Manual Hive Download Script
# This script downloads Hive manually with multiple fallback options

HIVE_VERSION="4.0.0"
DOWNLOAD_DIR="/tmp"
HIVE_FILE="apache-hive-${HIVE_VERSION}-bin.tar.gz"

echo "=========================================="
echo "Manual Hive Download Script"
echo "=========================================="
echo "Version: $HIVE_VERSION"
echo "Download directory: $DOWNLOAD_DIR"
echo ""

# Function to check if file exists and is valid
check_file() {
    local file_path="$1"
    if [ -f "$file_path" ] && [ -s "$file_path" ]; then
        echo "✓ File exists and is not empty: $file_path"
        echo "  Size: $(du -h "$file_path" | cut -f1)"
        return 0
    else
        echo "✗ File not found or empty: $file_path"
        return 1
    fi
}

# Function to download with wget
download_with_wget() {
    local url="$1"
    local output="$2"
    echo "Trying wget: $url"
    wget --timeout=300 --tries=3 --progress=bar:force -O "$output" "$url"
    return $?
}

# Function to download with curl
download_with_curl() {
    local url="$1"
    local output="$2"
    echo "Trying curl: $url"
    curl -L --connect-timeout 300 --max-time 1800 --progress-bar -o "$output" "$url"
    return $?
}

# Check if file already exists
if check_file "$DOWNLOAD_DIR/$HIVE_FILE"; then
    echo "Hive file already exists. Skipping download."
    exit 0
fi

echo "Starting download process..."
echo ""

# List of download URLs to try
URLS=(
    "https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/${HIVE_FILE}"
    "https://dlcdn.apache.org/hive/hive-${HIVE_VERSION}/${HIVE_FILE}"
    "https://mirrors.ocf.berkeley.edu/apache/hive/hive-${HIVE_VERSION}/${HIVE_FILE}"
    "https://mirror.olnevhost.net/pub/apache/hive/hive-${HIVE_VERSION}/${HIVE_FILE}"
)

# Try each URL with different methods
for url in "${URLS[@]}"; do
    echo "Attempting download from: $url"
    echo "----------------------------------------"
    
    # Try wget first
    if download_with_wget "$url" "$DOWNLOAD_DIR/$HIVE_FILE"; then
        if check_file "$DOWNLOAD_DIR/$HIVE_FILE"; then
            echo "✓ Download successful with wget!"
            exit 0
        fi
    fi
    
    # Try curl if wget failed
    if download_with_curl "$url" "$DOWNLOAD_DIR/$HIVE_FILE"; then
        if check_file "$DOWNLOAD_DIR/$HIVE_FILE"; then
            echo "✓ Download successful with curl!"
            exit 0
        fi
    fi
    
    echo "✗ Download failed from this URL"
    echo ""
done

echo "=========================================="
echo "All download attempts failed!"
echo "=========================================="
echo ""
echo "Manual download options:"
echo "1. Download from your local machine and upload via scp:"
echo "   scp apache-hive-${HIVE_VERSION}-bin.tar.gz team@176.109.91.5:/tmp/"
echo ""
echo "2. Use a different mirror or version"
echo ""
echo "3. Check your internet connection"
echo ""
echo "Available mirrors:"
for url in "${URLS[@]}"; do
    echo "  - $url"
done

exit 1
