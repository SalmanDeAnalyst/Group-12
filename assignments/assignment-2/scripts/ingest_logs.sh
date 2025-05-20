#!/bin/bash

# Accept date parameter
DATE=$1

# Extract year, month, and day
YEAR=$(echo $DATE | cut -d'-' -f1)
MONTH=$(echo $DATE | cut -d'-' -f2)
DAY=$(echo $DATE | cut -d'-' -f3)

# Define Local and HDFS Paths
LOCAL_LOG_DIR="/data/logs/$DATE"
LOCAL_METADATA_DIR="/data/metadata/$DATE"

HDFS_LOG_DIR="/raw/logs/$YEAR/$MONTH/$DAY"
HDFS_METADATA_DIR="/raw/metadata/$YEAR/$MONTH/$DAY"

# Ensure HDFS directories exist
hdfs dfs -mkdir -p $HDFS_LOG_DIR
hdfs dfs -mkdir -p $HDFS_METADATA_DIR

# Copy logs to HDFS
if [ "$(ls -A $LOCAL_LOG_DIR 2>/dev/null)" ]; then
    hdfs dfs -put $LOCAL_LOG_DIR/* $HDFS_LOG_DIR/
    echo "Logs copied to HDFS: $HDFS_LOG_DIR"
else
    echo "No log files found for $DATE in $LOCAL_LOG_DIR"
fi

# Copy metadata to HDFS
if [ "$(ls -A $LOCAL_METADATA_DIR 2>/dev/null)" ]; then
    hdfs dfs -put $LOCAL_METADATA_DIR/* $HDFS_METADATA_DIR/
    echo "Metadata copied to HDFS: $HDFS_METADATA_DIR"
else
    echo "No metadata files found for $DATE in $LOCAL_METADATA_DIR"
fi

echo "Ingestion completed for date: $DATE"

