# Spark with Python dependencies for PySpark jobs
FROM apache/spark:3.5.0

USER root

# Install Python development tools and required packages
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip3 install \
    pyspark==3.5.0 \
    psycopg2-binary==2.9.9

# Switch back to spark user
USER spark

WORKDIR /opt/spark
