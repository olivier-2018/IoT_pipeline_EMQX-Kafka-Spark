#!/bin/bash
# Spark Environment Configuration for IoT Data Pipeline

export SPARK_DAEMON_MEMORY=256m
export SPARK_DAEMON_JAVA_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=20"
export SPARK_WORKER_CORES=1
export SPARK_WORKER_MEMORY=1g
export SPARK_DRIVER_MEMORY=512m
export SPARK_EXECUTOR_MEMORY=1g
export SPARK_EXECUTOR_CORES=1

# Master/Worker settings
export SPARK_MASTER_HOST=spark-master
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

# Performance tuning
export SPARK_DRIVER_EXTRA_JAVA_OPTIONS="-XX:+UseG1GC -XX:MaxGCPauseMillis=20"
export SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS="-XX:+UseG1GC -XX:MaxGCPauseMillis=20"

# Networking
export SPARK_LOCAL_IP=0.0.0.0
