# 1. Use the official Spark image (Pre-built with Spark 3.4.1, Java, and Python)
FROM apache/spark:3.4.1-python3

# 2. Switch to root user to install dependencies
USER root

# 3. Set working directory
WORKDIR /app

# 4. Copy requirements and install Python dependencies
# We assume requirements.txt contains: cassandra-driver
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. OPTIONAL BUT RECOMMENDED: Pre-download the connector JARs
# This prevents the script from downloading them every time it runs.
# We run a dummy command to trigger the download into the ~/.ivy2/cache
RUN /opt/spark/bin/spark-submit \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,mysql:mysql-connector-java:8.0.33 \
    --class org.apache.spark.examples.SparkPi \
    /opt/spark/examples/jars/spark-examples_2.12-3.4.1.jar 1 > /dev/null 2>&1 || true

# 6. Copy your ETL script
COPY app /app

# 7. Set the entrypoint
CMD ["python3", "/app/jobs/etl_pipeline.py"]