#!/bin/bash

# Define the root project directory
PROJECT_ROOT="$(pwd)"

echo "Creating directory structure for $PROJECT_ROOT..."

# Create directories
mkdir -p $PROJECT_ROOT/conf
mkdir -p $PROJECT_ROOT/datasets/raw_data
mkdir -p $PROJECT_ROOT/datasets/delta_tables
mkdir -p $PROJECT_ROOT/database
mkdir -p $PROJECT_ROOT/spark_apps
mkdir -p $PROJECT_ROOT/spark_events
mkdir -p $PROJECT_ROOT/prometheus_data
mkdir -p $PROJECT_ROOT/telemetry_etl

# Create empty configuration files
touch $PROJECT_ROOT/conf/spark-defaults.conf
touch $PROJECT_ROOT/conf/metrics.properties
touch $PROJECT_ROOT/conf/prometheus.yml

# Create standard empty files
touch $PROJECT_ROOT/docker-compose.yml
touch $PROJECT_ROOT/requirements.txt
touch $PROJECT_ROOT/spark_apps/test_job.py
touch $PROJECT_ROOT/telemetry_etl/extract_history.py

# Set permissions for Docker volumes so containers can write to them
chmod -R 777 $PROJECT_ROOT/spark_events
chmod -R 777 $PROJECT_ROOT/datasets
chmod -R 777 $PROJECT_ROOT/prometheus_data
echo "Done! Navigate to cd $PROJECT_ROOT to get started."


docker exec -it spark-master spark-submit \
/opt/spark/spark_apps/generate_delta_data.py

docker exec -it spark-master spark-submit \
  /opt/spark/run_telemetry_gauntlet.py

docker exec -it spark-master spark-submit \
/opt/spark/stress_test_gauntlet.py