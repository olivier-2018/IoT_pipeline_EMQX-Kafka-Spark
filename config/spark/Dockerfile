# Spark with Kafka connector + PostgreSQL JDBC driver baked in for PySpark jobs
FROM apache/spark:3.5.0

USER root

# zip is needed by scripts/submit-spark-jobs.sh to package spark-structured-streaming-jobs/shared_utils
# for distribution to executors via --py-files
RUN apt-get update && apt-get install -y \
    curl \
    zip \
    && rm -rf /var/lib/apt/lists/*

# Bake in the Kafka connector (structured streaming) and PostgreSQL JDBC driver so
# spark-submit never needs network access to Maven Central at job-submit time.
# Each jar is verified against its official Maven Central sha1 checksum, failing the
# build on any mismatch.
RUN set -eu; \
    fetch_jar() { \
        group_path="$1"; artifact="$2"; version="$3"; \
        base_url="https://repo1.maven.org/maven2/${group_path}/${artifact}/${version}/${artifact}-${version}.jar"; \
        dest="/opt/spark/jars/${artifact}-${version}.jar"; \
        curl -fsSL -o "$dest" "$base_url"; \
        curl -fsSL -o "${dest}.sha1" "${base_url}.sha1"; \
        echo "$(cat "${dest}.sha1")  $dest" | sha1sum -c -; \
        rm "${dest}.sha1"; \
    }; \
    fetch_jar org/apache/spark spark-sql-kafka-0-10_2.12 3.5.0; \
    fetch_jar org/apache/spark spark-token-provider-kafka-0-10_2.12 3.5.0; \
    fetch_jar org/apache/kafka kafka-clients 3.4.1; \
    fetch_jar org/apache/commons commons-pool2 2.11.1; \
    fetch_jar org/postgresql postgresql 42.6.0; \
    chown spark:spark /opt/spark/jars/*.jar

USER spark

WORKDIR /opt/spark
