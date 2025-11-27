#!/bin/bash

# Create admin user directory
echo "Creating /user/admin directory..."
hdfs dfs -mkdir -p /user/admin

# Set ownership
echo "Setting ownership to admin:supergroup..."
hdfs dfs -chown -R admin:supergroup /user/admin

echo "HDFS setup completed successfully!"

# Wait for the namenode process to exit (keep container running)
wait $NAMENODE_PID
